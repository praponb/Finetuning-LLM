WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, v
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS has_fault
    FROM flattened
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        service,
        log_ts,
        v,
        has_fault,
        DATEDIFF(CURRENT_DATE(), log_ts) / 7 AS weeks_since_log_ts,
        status_code_high + has_fault AS status_code_high_plus_has_fault
    FROM unpacked
    WHERE log_ts > DATE '2025-06-01'
      AND latency_ms != 1200
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        service,
        status_code_high_plus_has_fault,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY has_fault) AS has_fault_dense_rank,
        RANK() OVER (ORDER BY status_code_high_plus_has_fault DESC) AS status_code_high_plus_has_fault_rank,
        STDDEV_SAMP(has_fault) OVER (PARTITION BY service) AS service_stddev_samp_has_fault
    FROM cleaned
),
enriched AS (
    -- Ranking and gap features
    SELECT
        status_code_high_plus_has_fault,
        has_fault_dense_rank,
        status_code_high_plus_has_fault_rank,
        service_stddev_samp_has_fault,
        MAX(status_code_high_plus_has_fault_rank) OVER w_hist AS service_max_status_code_high_plus_has_fault_rank,
        ROUND(has_fault_dense_rank / SUM(has_fault_dense_rank) OVER (PARTITION BY service), 4) AS has_fault_dense_rank_share,
        PERCENT_RANK() OVER w_hist_2 AS has_fault_dense_rank_percent_rank,
        MAX(status_code_high_plus_has_fault_rank) OVER w_hist AS service_max_status_code_high_plus_has_fault_rank_2,
        MAX(status_code_high_plus_has_fault_rank) OVER w_hist AS service_max_status_code_high_plus_has_fault_rank_3
    FROM scored
    WHERE status_code_high_plus_has_fault_rank < 8
    WINDOW w_hist AS (PARTITION BY service), w_hist_2 AS (PARTITION BY service ORDER BY has_fault_dense_rank NULLS LAST)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        status_code_high_plus_has_fault,
        service_stddev_samp_has_fault,
        service_max_status_code_high_plus_has_fault_rank,
        has_fault_dense_rank_share,
        service_max_status_code_high_plus_has_fault_rank_3,
        service_max_status_code_high_plus_has_fault_rank + has_fault_dense_rank_share AS service_max_status_code_high_plus_has_fault_rank_plus_has_fault_dense_rank_share,
        FLOOR(service_max_status_code_high_plus_has_fault_rank) AS service_max_status_code_high_plus_has_fault_rank_whole
    FROM enriched
    WHERE status_code_high_plus_has_fault < 33
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        GREATEST(service_max_status_code_high_plus_has_fault_rank_whole, service_stddev_samp_has_fault) AS greatest_service_max_status_code_high_plus_has_fault_rank_whole_service_stddev_samp_has_fault,
        COALESCE(has_fault_dense_rank_share, 0) AS has_fault_dense_rank_share_filled
    FROM base
    WHERE status_code_high_plus_has_fault BETWEEN 32 AND 75
)
SELECT
    service_stddev_samp_has_fault,
    service_max_status_code_high_plus_has_fault_rank,
    has_fault_dense_rank_share,
    service_max_status_code_high_plus_has_fault_rank_plus_has_fault_dense_rank_share,
    service_max_status_code_high_plus_has_fault_rank_whole,
    greatest_service_max_status_code_high_plus_has_fault_rank_whole_service_stddev_samp_has_fault
FROM valid
ORDER BY greatest_service_max_status_code_high_plus_has_fault_rank_whole_service_stddev_samp_has_fault;
