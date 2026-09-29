WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        ARRAY_CONTAINS(MAP_KEYS(context), 'region') AS has_region,
        COALESCE(context['region'], 'none') AS region_or_default,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        context['version'] AS version
    FROM app_logs
)
-- Window metrics over several different partitions
SELECT
    id,
    service,
    level,
    region_or_default,
    duration_text,
    version,
    LAST_VALUE(tier, TRUE) OVER w_hist last_known_tier,
    LAST_VALUE(version) IGNORE NULLS OVER (PARTITION BY level ORDER BY log_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_version,
    FIRST_VALUE(tier) IGNORE NULLS OVER (PARTITION BY service ORDER BY log_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_tier
FROM unpacked
WHERE tier IN ('free', 'pro', 'gold')
WINDOW w_hist AS (PARTITION BY DATE_TRUNC('MONTH', log_ts), service ORDER BY log_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
