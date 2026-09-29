WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        log_ts,
        latency_ms,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE latency_ms >= 1348
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(status_code_high) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_high_count_2h,
        DENSE_RANK() OVER (ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_dense_rank,
        DENSE_RANK() OVER w_part AS is_flagged_dense_rank,
        COUNT(is_flagged) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS is_flagged_count_15m
    FROM parsed
    WHERE status_code_high != 2
    WINDOW w_part AS (ORDER BY is_flagged)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        log_ts,
        latency_ms,
        is_flagged,
        is_flagged_dense_rank,
        RANK() OVER (ORDER BY is_flagged_count_15m NULLS FIRST) AS is_flagged_count_15m_rank,
        COUNT(is_flagged_count_15m) OVER w_recent AS is_flagged_count_15m_count_15m,
        MAX(is_flagged) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS is_flagged_max_30m,
        COUNT(latency_ms) OVER w_recent_2 AS latency_ms_count_2h,
        MAX(status_code_high) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_high_max_1h
    FROM with_windows
    WINDOW w_recent AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW), w_recent_2 AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TO_DATE(log_ts) AS log_ts_date,
        status_code_high_max_1h / NULLIF(is_flagged_max_30m, 0) AS status_code_high_max_1h_per_is_flagged_max_30m
    FROM enriched
    WHERE log_ts >= DATE '2025-06-01'
      AND latency_ms_count_2h > 15
),
report AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        is_flagged,
        is_flagged_dense_rank,
        is_flagged_count_15m_rank,
        is_flagged_count_15m_count_15m,
        is_flagged_max_30m,
        latency_ms_count_2h
    FROM cleaned
    ORDER BY is_flagged_count_15m_count_15m, latency_ms
)
SELECT
    id,
    log_ts,
    latency_ms,
    is_flagged,
    is_flagged_count_15m_count_15m,
    is_flagged_max_30m,
    latency_ms_count_2h
FROM report;
