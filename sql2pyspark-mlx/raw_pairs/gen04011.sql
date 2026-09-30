WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        latency_ms,
        status_code,
        key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged
    FROM flattened
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY key ORDER BY status_code DESC) AS rank_in_group
        FROM unpacked
    ) t
    WHERE rank_in_group = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        latency_ms,
        key,
        status_code_high,
        MAX(latency_ms) OVER (PARTITION BY key) AS key_max_latency_ms,
        FIRST_VALUE(status_code_high, TRUE) OVER (ORDER BY rank_in_group, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_code_high,
        NTILE(5) OVER (PARTITION BY key ORDER BY latency_ms, id, key) AS latency_ms_bucket
    FROM latest
)
-- Grouped metrics
SELECT
    key,
    CASE WHEN latency_ms_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_bucket_tier,
    SUM(CASE WHEN key = 'region' THEN first_known_status_code_high ELSE 0 END) AS region_first_known_status_code_high,
    COUNT(CASE WHEN key = 'trace' THEN 1 END) AS trace_count,
    COUNT(CASE WHEN key = 'trace' THEN 1 END) AS trace_count_2
FROM enriched
GROUP BY key, CASE WHEN latency_ms_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END
HAVING SUM(CASE WHEN key = 'region' THEN first_known_status_code_high ELSE 0 END) > 0;
