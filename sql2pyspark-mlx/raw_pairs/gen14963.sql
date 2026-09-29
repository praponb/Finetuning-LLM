WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        log_ts,
        level,
        key,
        v,
        CONCAT(host, '/', level) host_level
    FROM exploded
    WHERE key IS NOT NULL
      AND key IS DISTINCT FROM 'version'
),
base_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        id,
        key,
        v,
        CASE WHEN key = 'version' THEN 1 ELSE 0 END is_version
    FROM base
    WHERE key IS NOT NULL
      AND log_ts >= '2025-08-01'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        v,
        SUM(is_version) OVER (PARTITION BY key) key_sum_is_version
    FROM base_app_logs
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CAST(key_sum_is_version AS STRING) key_sum_is_version_num
    FROM with_windows
    WHERE key_sum_is_version <> 232
),
filtered_app_logs AS (
    -- Keep recent, valid rows
    SELECT *, CAST(key_sum_is_version AS DOUBLE) key_sum_is_version_num_2
    FROM filtered
    WHERE key_sum_is_version IS NOT NULL
)
SELECT
    id,
    v,
    key_sum_is_version,
    key_sum_is_version_num,
    key_sum_is_version_num_2
FROM filtered_app_logs
ORDER BY key_sum_is_version_num_2 DESC, id
