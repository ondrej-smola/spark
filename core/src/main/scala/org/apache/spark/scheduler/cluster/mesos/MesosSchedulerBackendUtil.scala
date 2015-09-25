/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster.mesos

import com.fasterxml.jackson.databind.util.ContainerBuilder
import org.apache.mesos.Protos
import org.apache.mesos.Protos.ContainerInfo.DockerInfo.Network
import org.apache.mesos.Protos.{Parameter, ContainerInfo, Volume}
import org.apache.mesos.Protos.ContainerInfo.DockerInfo

import org.apache.spark.{Logging, SparkConf}

/**
 * A collection of utility functions which can be used by both the
 * MesosSchedulerBackend and the CoarseMesosSchedulerBackend.
 */
private[mesos] object MesosSchedulerBackendUtil extends Logging {
  /**
   * Parse a comma-delimited list of volume specs, each of which
   * takes the form [host-dir:]container-dir[:rw|:ro].
   */
  def parseVolumesSpec(volumes: String): List[Volume] = {
    volumes.split(",").map(_.split(":")).flatMap { spec =>
      val vol: Volume.Builder = Volume
        .newBuilder()
        .setMode(Volume.Mode.RW)
      spec match {
        case Array(container_path) =>
          Some(vol.setContainerPath(container_path))
        case Array(container_path, "rw") =>
          Some(vol.setContainerPath(container_path))
        case Array(container_path, "ro") =>
          Some(vol.setContainerPath(container_path)
            .setMode(Volume.Mode.RO))
        case Array(host_path, container_path) =>
          Some(vol.setContainerPath(container_path)
            .setHostPath(host_path))
        case Array(host_path, container_path, "rw") =>
          Some(vol.setContainerPath(container_path)
            .setHostPath(host_path))
        case Array(host_path, container_path, "ro") =>
          Some(vol.setContainerPath(container_path)
            .setHostPath(host_path)
            .setMode(Volume.Mode.RO))
        case _ => {
          logWarning(s"Unable to parse volume specs: $volumes. "
            + "Expected form: \"[host-dir:]container-dir[:rw|:ro](, ...)\"")
          None
        }
      }
    }
      .map {
      _.build()
    }
      .toList
  }

  /**
   * Parse a comma-delimited list of port mapping specs, each of which
   * takes the form host_port:container_port[:udp|:tcp]
   *
   * Note:
   * the docker form is [ip:]host_port:container_port, but the DockerInfo
   * message has no field for 'ip', and instead has a 'protocol' field.
   * Docker itself only appears to support TCP, so this alternative form
   * anticipates the expansion of the docker form to allow for a protocol
   * and leaves open the chance for mesos to begin to accept an 'ip' field
   */
  def parsePortMappingsSpec(portmaps: String): List[DockerInfo.PortMapping] = {
    portmaps.split(",").map(_.split(":")).flatMap { spec: Array[String] =>
      val portmap: DockerInfo.PortMapping.Builder = DockerInfo.PortMapping
        .newBuilder()
        .setProtocol("tcp")
      spec match {
        case Array(host_port, container_port) =>
          Some(portmap.setHostPort(host_port.toInt)
            .setContainerPort(container_port.toInt))
        case Array(host_port, container_port, protocol) =>
          Some(portmap.setHostPort(host_port.toInt)
            .setContainerPort(container_port.toInt)
            .setProtocol(protocol))
        case spec => {
          logWarning(s"Unable to parse port mapping specs: $portmaps. "
            + "Expected form: \"host_port:container_port[:udp|:tcp](, ...)\"")
          None
        }
      }
    }
      .map {
      _.build()
    }
      .toList
  }

  /**
   * Parses docker network type string
   */
  def parseNetworkTypeSpec(netType: String): Option[ContainerInfo.DockerInfo.Network] = {
    netType match {
      case "HOST" => Some(Network.HOST)
      case "BRIDGE" => Some(Network.BRIDGE)
      case "NONE" => Some(Network.NONE)
      case _ =>
        logWarning(s"Unable to parse network type specs: $netType. Expected \"HOST\", \"BRIDGE\" or \"NONE\"")
        None
    }
  }


  /**
   * Returns all properties starting with spark.mesos.executor.docker.parameter as Parameter instance
   *
   * @param configuration Where to search
   * @return Found occurrences
   */
  def listAdditionalExecutorParameters(configuration: Map[String, String]): List[Parameter] = {
    configuration.filter(_._1.startsWith("spark.mesos.executor.docker.parameter."))
      .map(_._1.stripPrefix("spark.mesos.executor.docker.parameter."))
      .map { case (k, v) => Parameter.newBuilder().setKey(k).setValue(v).build() } toList
  }


  /**
   * Configure provided container builder to represent docker container with image, network, port mappings, volumes and
   * docker cli parameters. All configuration is read from provided configuration source and uses protobuf default value
   * when optional section is missing. Configuration only takes place when docker image configuration property is found,
   * otherwise it does nothing and None is returned
   * 
   * @param builder Container builder to configure
   * @param configuration Property container
   * @return Configured ContainerInfo.Builder when configuration contains docker image name property or None when
   *         section is missing or does not contain docker image name
   */
  def setupDockerContainerBuilderFromConfiguration(
                                builder: ContainerInfo.Builder,
                                configuration: Map[String, String]): Option[ContainerInfo.Builder] = {

    configuration.get("spark.mesos.executor.docker.image") match {          
      case Some(imageName) =>
        builder.setType(ContainerInfo.Type.DOCKER)

        val network = configuration.get("spark.mesos.executor.docker.network").flatMap(parseNetworkTypeSpec)
        val portmaps = configuration.get("spark.mesos.executor.docker.portmaps").map(parsePortMappingsSpec)
        val volumes = configuration.get("spark.mesos.executor.docker.volumes").map(parseVolumesSpec)
        val additionalParameters = listAdditionalExecutorParameters(configuration)

        val docker = ContainerInfo.DockerInfo.newBuilder().setImage(imageName)

        network.foreach(docker.setNetwork)
        portmaps.foreach(_.foreach(docker.addPortMappings))
        volumes.foreach(_.foreach(builder.addVolumes))
        additionalParameters.foreach(docker.addParameters)

        builder.setDocker(docker.build())

        logDebug("Container builder configured for docker image " + imageName)
        Some(builder)
      case None => None
    }
  }
}
