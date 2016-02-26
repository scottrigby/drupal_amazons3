<?php

namespace Drupal\amazons3;

/**
 * @file
 * A wrapper around S3Client::factory() using aws_key / aws_secret variables.
 */

use Aws\Common\Credentials\Credentials;
use Aws\S3\S3Client as AwsS3Client;
use Capgemini\Cache\DrupalDoctrineCache;
use Doctrine\Common\Cache\CacheProvider;
use Doctrine\Common\Cache\ArrayCache;
use Doctrine\Common\Cache\ChainCache;
use Drupal\amazons3\Exception\S3ConnectValidationException;
use Drupal\amazons3\StreamWrapperConfiguration;
use Guzzle\Common\Collection;
use Guzzle\Service\Command\Factory\AliasFactory;

/**
 * A wrapper around S3Client::factory() using aws_key / aws_secret variables.
 *
 * @class S3Client
 *
 * @package Drupal\amazons3
 */
class S3Client {
  use DrupalAdapter\Bootstrap;

  /**
   * @var CacheProvider An optional cache for stat calls.
   */
  protected static $cache;

  /**
   * @var bool|int The lifetime of stat cache calls, or false for no expiry.
   */
  protected static $cacheLifetime;

  /**
   * This set of commandAliases is a protected static with no getter on the
   * S3Client class.
   *
   * @var array
   */
  protected static $commandAliases = array(
    // REST API Docs Aliases
    'GetService' => 'ListBuckets',
    'GetBucket'  => 'ListObjects',
    'PutBucket'  => 'CreateBucket',

    // SDK 1.x Aliases
    'GetBucketHeaders'              => 'HeadBucket',
    'GetObjectHeaders'              => 'HeadObject',
    'SetBucketAcl'                  => 'PutBucketAcl',
    'CreateObject'                  => 'PutObject',
    'DeleteObjects'                 => 'DeleteMultipleObjects',
    'PutObjectCopy'                 => 'CopyObject',
    'SetObjectAcl'                  => 'PutObjectAcl',
    'GetLogs'                       => 'GetBucketLogging',
    'GetVersioningStatus'           => 'GetBucketVersioning',
    'SetBucketPolicy'               => 'PutBucketPolicy',
    'CreateBucketNotification'      => 'PutBucketNotification',
    'GetBucketNotifications'        => 'GetBucketNotification',
    'CopyPart'                      => 'UploadPartCopy',
    'CreateWebsiteConfig'           => 'PutBucketWebsite',
    'GetWebsiteConfig'              => 'GetBucketWebsite',
    'DeleteWebsiteConfig'           => 'DeleteBucketWebsite',
    'CreateObjectExpirationConfig'  => 'PutBucketLifecycle',
    'GetObjectExpirationConfig'     => 'GetBucketLifecycle',
    'DeleteObjectExpirationConfig'  => 'DeleteBucketLifecycle',
  );

  /**
   * Create a new S3Client using aws_key / aws_secret $conf variables.
   *
   * @param array|Collection $config
   *   An array of configuration options to pass to \Aws\S3\S3Client::factory().
   *   If 'credentials' are set they will be used instead of aws_key and
   *   aws_secret.
   * @param string $bucket
   *   (optional) The bucket to associate this client with. If empty, the client
   *   will default to $config['region'], and then the us-east-1 region.
   *
   * @return \Aws\S3\S3Client
   */
  public static function factory($config = array(), $bucket = NULL) {
    // Attach doctrine cache for validation here, because this method is always
    // called to get the $client param of other methods in this class.
    $stream_wrapper_config = StreamWrapperConfiguration::fromDrupalVariables();
    if ($stream_wrapper_config->isCaching() && !static::$cache) {
      static::attachCache(
        new ChainCache([new ArrayCache(), new DrupalDoctrineCache()]),
        $stream_wrapper_config->getCacheLifetime()
      );
    }

    if (!isset($config['credentials'])) {
      $config['credentials'] = new Credentials(static::variable_get('amazons3_key'), static::variable_get('amazons3_secret'));
    }

    if (!isset($config['endpoint'])) {
      $endpoint = static::variable_get('amazons3_hostname');
      if(!empty($endpoint)){
        $config['endpoint'] = $endpoint;
      }
    }

    $curl_defaults = array(
      CURLOPT_CONNECTTIMEOUT => 30,
    );

    if (!isset($config['curl.options'])) {
      $config['curl.options'] = array();
    }

    $config['curl.options'] += $curl_defaults;

    // Set the default client location to the associated bucket.
    if (!isset($config['region'])) {
      $region = static::variable_get('amazons3_region');
      if (!empty($region)) {
        $config['region'] = $region;
      }
    }

    $client = AwsS3Client::factory($config);

    static::setCommandFactory($client);

    return $client;
  }

  /**
   * Attach a cache to this client.
   *
   * @param CacheProvider $cache
   * @param bool|int $lifetime
   *   (optional) Lifetime of cache items, defaults to no expiry.
   */
  public static function attachCache(CacheProvider $cache, $lifetime = false) {
    static::$cache = $cache;
    static::$cacheLifetime = $lifetime;
  }

  /**
   * Validate that a bucket exists.
   *
   * Since bucket names are global across all of S3, we can't determine if a
   * bucket doesn't exist at all, or if it exists but is owned by another S3
   * account.
   *
   * @param string $bucket
   *   The name of the bucket to test.
   * @param \Aws\S3\S3Client $client
   *   The S3Client to use.
   *
   * @throws S3ConnectValidationException
   *   Thrown when credentials are invalid or the bucket does not exist.
   */
  public static function validateBucketExists($bucket, AwsS3Client $client) {
    $key = 'bucket:' . $bucket;
    // Do not bother to fetch, because we only cache a successful response.
    if (!static::$cache->contains($key)) {
      if ($client->doesBucketExist($bucket, FALSE)) {
        static::$cache->save($key, TRUE, static::$cacheLifetime);
      }
      else {
        throw new S3ConnectValidationException('The S3 access credentials are invalid or the bucket does not exist.');
      }
    }
  }

  /**
   * Override the command factory on a client.
   *
   * @param \Aws\S3\S3Client $client
   *   The client to override.
   *
   * @codeCoverageIgnore
   */
  protected static function setCommandFactory($client) {
    $default = CompositeFactory::getDefaultChain($client);
    $default->add(
      new AliasFactory($client, static::$commandAliases),
      'Guzzle\Service\Command\Factory\ServiceDescriptionFactory'
    );
    $client->setCommandFactory($default);
  }

  /**
   * Get the region for an S3Client for a specific bucket.
   *
   * @param string $bucket
   *   The bucket to get the region for.
   * @param \Aws\S3\S3Client $client
   *   The S3Client to use.
   *
   * @return string
   *   The region for the bucket.
   */
  public static function getBucketLocation($bucket, AwsS3Client $client) {
    return $client->getBucketLocation(array('Bucket' => $bucket))->get('Location');
  }
}
