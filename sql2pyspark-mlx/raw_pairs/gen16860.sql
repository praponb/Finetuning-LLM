WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, value
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(latency_ms, 1) AS latency_ms_rounded
    FROM exploded
    WHERE log_ts > DATE '2025-06-01'
      AND k IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts ASC, id ASC, k ASC) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        k,
        value,
        latency_ms_rounded,
        SUM(session_start_flag) OVER (PARTITION BY level ORDER BY log_ts ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k, level ORDER BY latency_ms_rounded ASC NULLS FIRST, id ASC, k ASC) AS rank_in_group
    FROM sessions_app_logs
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        log_ts,
        k,
        rank_in_group,
        COUNT(latency_ms_rounded) OVER (PARTITION BY k ORDER BY session_no ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_latency_ms_rounded,
        AVG(latency_ms_rounded) OVER (PARTITION BY k ORDER BY session_no ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_latency_ms_rounded,
        MAX(latency_ms_rounded) OVER (PARTITION BY k ORDER BY session_no ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_latency_ms_rounded
    FROM ranked
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts ASC, id ASC, k ASC) > INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM enriched
),
sessions_app_logs_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY k ORDER BY log_ts ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps_2
)
SELECT
    id,
    k,
    rank_in_group,
    running_count_latency_ms_rounded,
    running_avg_latency_ms_rounded,
    running_max_latency_ms_rounded,
    CASE WHEN k RLIKE '^(trace|version)$' THEN 'CORE' WHEN k RLIKE '^(region)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS k_zone
FROM sessions_app_logs_2
ORDER BY id DESC, running_avg_latency_ms_rounded ASC
