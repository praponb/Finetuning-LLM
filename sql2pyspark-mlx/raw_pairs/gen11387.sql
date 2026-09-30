SELECT
    id,
    host,
    FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
    ARRAY_CONTAINS(MAP_KEYS(context), 'version') AS has_version,
    COALESCE(context['version'], 'unknown') AS version_or_default
FROM app_logs
WHERE host IN ('h3', 'h4');
