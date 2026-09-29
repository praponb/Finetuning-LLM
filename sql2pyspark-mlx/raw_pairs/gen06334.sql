WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, value
),
unpacked AS (
    -- Semi-structured parsing
    SELECT
        service,
        log_ts,
        k,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged
    FROM exploded
),
scored AS (
    -- Running and moving aggregates
    SELECT
        service,
        log_ts,
        k,
        DENSE_RANK() OVER (ORDER BY status_code_high DESC) AS status_code_high_dense_rank,
        DENSE_RANK() OVER (PARTITION BY service, k ORDER BY status_code_high NULLS LAST) AS status_code_high_dense_rank_2,
        CUME_DIST() OVER (PARTITION BY k ORDER BY is_flagged DESC) AS is_flagged_cume_dist,
        MAX(is_flagged) OVER (PARTITION BY k, service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS is_flagged_max_1h
    FROM unpacked
    WHERE status_code_high != 2
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        service,
        log_ts,
        status_code_high_dense_rank_2,
        is_flagged_max_1h,
        GREATEST(status_code_high_dense_rank_2, is_flagged_max_1h) AS greatest_status_code_high_dense_rank_2_is_flagged_max_1h,
        ROUND(is_flagged_max_1h / status_code_high_dense_rank_2, 2) AS is_flagged_max_1h_per_status_code_high_dense_rank_2,
        COALESCE(k, 'NONE') AS k_clean
    FROM scored
    WHERE log_ts > TO_DATE('2025-06-01')
      AND k IS DISTINCT FROM 'version'
),
matched_keys AS (
    -- Set difference on keys
    SELECT service
    FROM filtered
    EXCEPT DISTINCT
    SELECT service
    FROM app_logs
    WHERE TO_DATE(log_ts) >= '2024-11-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.service = k.service
)
SELECT
    service,
    log_ts,
    status_code_high_dense_rank_2,
    greatest_status_code_high_dense_rank_2_is_flagged_max_1h,
    is_flagged_max_1h_per_status_code_high_dense_rank_2,
    k_clean
FROM verified
ORDER BY status_code_high_dense_rank_2 DESC NULLS LAST, greatest_status_code_high_dense_rank_2_is_flagged_max_1h DESC;
