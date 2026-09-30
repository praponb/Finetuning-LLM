WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, v
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(latency_ms) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) latency_ms_sum_1h,
        CUME_DIST() OVER (PARTITION BY level, attr_key ORDER BY latency_ms ASC NULLS FIRST) latency_ms_cume_dist,
        COUNT(latency_ms) OVER (PARTITION BY host) host_count_latency_ms
    FROM flattened
    WHERE status_code BETWEEN 398 AND 414
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        latency_ms_sum_1h,
        latency_ms_cume_dist,
        host_count_latency_ms,
        LENGTH(attr_key) attr_key_length
    FROM scored
    WHERE log_ts > TO_TIMESTAMP('2025-06-01 07:00:00')
      AND log_ts < '2025-05-05'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, level
    FROM cleaned
    INTERSECT ALL
    SELECT id, level
    FROM app_logs
    WHERE log_ts >= '2025-03-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.level = k.level
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        host,
        attr_key,
        latency_ms_sum_1h
    FROM reconciled
) src
PIVOT (
    AVG(latency_ms_sum_1h) AS amount
    FOR attr_key IN ('version', 'trace')
);
