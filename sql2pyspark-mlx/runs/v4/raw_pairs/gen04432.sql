WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN key IN ('version', 'region') THEN 'Core' ELSE 'Other' END AS key_group,
        SUBSTRING(service, 1, 4) AS service_prefix
    FROM exploded
    WHERE service NOT IN ('billing', 'auth', 'search')
),
remaining_keys AS (
    -- Set difference on keys
    SELECT host, id
    FROM base
    EXCEPT ALL
    SELECT host, id
    FROM app_logs
    WHERE log_ts < '2024-03-17'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.host = k.host AND s.id = k.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT host, id
    FROM kept_rows
    INTERSECT ALL
    SELECT host, id
    FROM app_logs
    WHERE status_code <> 353
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN matched_keys k ON s.host = k.host AND s.id = k.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER w_recent AS status_code_percent_rank,
        DENSE_RANK() OVER w_recent AS status_code_dense_rank
    FROM verified
    WINDOW w_recent AS (PARTITION BY key ORDER BY status_code DESC)
),
with_windows_app_logs AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        key,
        v,
        key_group,
        service_prefix,
        status_code_percent_rank,
        status_code_dense_rank,
        CUME_DIST() OVER (PARTITION BY service ORDER BY status_code_dense_rank DESC NULLS FIRST) AS status_code_dense_rank_cume_dist,
        AVG(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_avg_15m,
        STDDEV_SAMP(status_code_dense_rank) OVER (PARTITION BY key) AS key_stddev_samp_status_code_dense_rank,
        CUME_DIST() OVER w_hist AS status_code_cume_dist,
        MIN(status_code_dense_rank) OVER (PARTITION BY service) AS service_min_status_code_dense_rank
    FROM with_windows
    WINDOW w_hist AS (ORDER BY status_code DESC)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_code_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_dense_rank_tier,
    ROUND(AVG(service_min_status_code_dense_rank), 2) AS avg_service_min_status_code_dense_rank,
    COUNT(CASE WHEN service = 'billing' THEN 1 END) AS billing_count,
    MIN(log_ts) AS started_at,
    ROUND(SUM(status_code_avg_15m) / NULLIF(COUNT(CASE WHEN key = 'trace' THEN 1 END), 0), 2) AS status_code_avg_15m_per_trace,
    COUNT_IF(key = 'version') AS version_events
FROM with_windows_app_logs
GROUP BY CASE WHEN status_code_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END;
