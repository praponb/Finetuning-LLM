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
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        CAST((status_code & 1) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        host,
        status_code,
        k,
        status_code_high,
        LEAD(has_fault) OVER w_grp AS next_has_fault,
        LAG(has_fault) OVER w_grp AS prev_has_fault,
        LEAD(status_code) OVER w_grp AS next_status_code,
        LAG(status_code) OVER w_grp AS prev_status_code,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM parsed
    WHERE status_code != 332
    WINDOW w_grp AS (PARTITION BY host, k ORDER BY latency_ms ASC, id ASC, k ASC)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(prev_has_fault) IGNORE NULLS OVER (ORDER BY next_has_fault ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_has_fault,
        ROW_NUMBER() OVER w_running AS seq_no_2,
        CUME_DIST() OVER (PARTITION BY host ORDER BY seq_no ASC NULLS LAST) AS seq_no_cume_dist,
        CUME_DIST() OVER (ORDER BY next_status_code ASC) AS next_status_code_cume_dist,
        SUM(status_code_high) OVER w_part AS running_status_code_high
    FROM metrics
    WINDOW w_running AS (PARTITION BY host, k ORDER BY next_status_code ASC, id ASC, k ASC), w_part AS (ORDER BY seq_no ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(prev_status_code) OVER (PARTITION BY service) AS service_avg_prev_status_code,
        SUM(status_code_high) OVER (ORDER BY next_status_code_cume_dist ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code_high,
        SUM(last_known_prev_has_fault) OVER (PARTITION BY k ORDER BY seq_no ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last_known_prev_has_fault
    FROM enriched
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(next_status_code_cume_dist AS INT) AS next_status_code_cume_dist_int,
        CAST(next_status_code_cume_dist AS INT) AS next_status_code_cume_dist_int_2
    FROM scored
    WHERE seq_no_2 < 6
)
SELECT
    id,
    service,
    status_code,
    k,
    status_code_high,
    next_has_fault,
    prev_has_fault,
    next_status_code,
    last_known_prev_has_fault
FROM cleaned;
