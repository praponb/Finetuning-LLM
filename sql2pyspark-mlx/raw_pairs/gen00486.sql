WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, attr_value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        service,
        log_ts,
        key,
        attr_value,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
base AS (
    -- Keep recent, valid rows
    SELECT
        service,
        log_ts,
        key,
        is_flagged,
        status_code_high,
        COALESCE(key, 'unassigned') AS key_filled,
        COALESCE(key, 'NONE') AS key_clean
    FROM unpacked
    WHERE service <> 'checkout'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN status_code_high >= 2 THEN 'High' WHEN status_code_high >= 1 THEN 'Medium' ELSE 'Low' END AS status_code_high_tier
    FROM base
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 15:00:00')
      AND key IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('WEEK', log_ts), key ORDER BY is_flagged DESC NULLS FIRST) AS is_flagged_cume_dist,
        AVG(is_flagged) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS is_flagged_avg_15m
    FROM filtered
),
with_windows_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY MONTH(log_ts), service ORDER BY is_flagged DESC) AS is_flagged_cume_dist_2,
        RANK() OVER (PARTITION BY key ORDER BY is_flagged ASC) AS is_flagged_rank,
        AVG(status_code_high) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_high_avg_30m
    FROM with_windows
)
SELECT
    key,
    is_flagged,
    key_filled,
    is_flagged_cume_dist,
    is_flagged_cume_dist_2,
    is_flagged_rank,
    status_code_high_avg_30m,
    CASE WHEN key RLIKE '^(trace|region)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS key_zone
FROM with_windows_app_logs;
