<?php

namespace InfluxDB2Test;

use InfluxDB2\Client;
use InfluxDB2\Model\WritePrecision;
use InfluxDB2\UdpWriter;
use PHPUnit\Framework\TestCase;

/**
 * Class WriteUdpTest
 * @package InfluxDB2Test
 */
class WriteUdpTest extends TestCase
{
    protected $baseConfig = [
        "url" => "http://useless:8086",
        "token" => "my-token",
        "bucket" => "my-bucket",
        "precision" => WritePrecision::NS,
        "org" => "my-org",
        "logFile" => "php://output"
    ];

    protected $dataString = 'h2o,location=west value=33i 15';

    protected function getWriterMock()
    {
        $method = new \ReflectionMethod(UdpWriter::class, 'writeSocket');
        $method->setAccessible(true);
        return $this->getMockBuilder(UdpWriter::class)
            ->setMethods(['writeSocket'])
            ->setConstructorArgs([$this->baseConfig + ['udpPort' => 1000]])
            ->getMock();
    }

    public function testRequireOptions()
    {
        $client = new Client($this->baseConfig);
        $this->expectException(\Exception::class);
        $client->createUdpWriter();
    }

    public function testSocketError()
    {
        $writer = $this->getWriterMock();
        $writer->method('writeSocket')->willReturn(false);
        $this->expectException(\Exception::class);
        $writer->write($this->dataString);
    }

    public function testLineProtocol()
    {
        $writer = $this->getWriterMock();
        $writer->method('writeSocket')->willReturn(10);
        $writer->write($this->dataString);
        $this->assertEquals($this->dataString, $writer->getLastPayload());
    }

}
