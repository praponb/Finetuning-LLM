WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CORR(latency_ms, status_code) OVER (PARTITION BY key, level) AS corr_latency_ms_status_code,
        SUM(latency_ms) OVER (PARTITION BY key) AS key_sum_latency_ms
    FROM exploded
),
matched_keys AS (
    -- Set intersection on keys
    SELECT level, id
    FROM scored
    INTERSECT ALL
    SELECT level, id
    FROM app_logs
    WHERE status_code >= 409
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.level = k.level AND s.id = k.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        key,
        attr_value,
        corr_latency_ms_status_code,
        key_sum_latency_ms,
        RANK() OVER (ORDER BY latency_ms DESC) AS latency_ms_rank,
        ROUND(key_sum_latency_ms / SUM(key_sum_latency_ms) OVER (PARTITION BY level), 4) AS key_sum_latency_ms_share,
        RANK() OVER w_running AS status_code_rank,
        CORR(status_code, latency_ms) OVER (PARTITION BY level) AS corr_status_code_latency_ms
    FROM verified
    WINDOW w_running AS (ORDER BY status_code DESC)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        level,
        corr_latency_ms_status_code,
        key_sum_latency_ms_share,
        corr_status_code_latency_ms
    FROM metrics
) src
UNPIVOT (
    value FOR metric_name IN (corr_latency_ms_status_code AS CORR_LATENCY_MS_STATUS_CODE, key_sum_latency_ms_share AS KEY_SUM_LATENCY_MS_SHARE, corr_status_code_latency_ms AS CORR_STATUS_CODE_LATENCY_MS)
);
