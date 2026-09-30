WITH lines AS (
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
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        service,
        latency_ms,
        k,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        service,
        flag_bit1,
        STDDEV_SAMP(flag_bit1) OVER w_running AS k_stddev_samp_flag_bit1,
        MIN(status_code_high) OVER (PARTITION BY k, service) AS k_min_status_code_high,
        MAX(latency_ms) OVER w_running AS k_max_latency_ms
    FROM unpacked
    WHERE status_code_high <= 2
    WINDOW w_running AS (PARTITION BY k)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, k_min_status_code_high + k_max_latency_ms AS k_min_status_code_high_plus_k_max_latency_ms
    FROM scored
    WHERE service NOT IN ('checkout', 'auth', 'search')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        k_stddev_samp_flag_bit1,
        k_min_status_code_high,
        k_min_status_code_high_plus_k_max_latency_ms,
        RANK() OVER (PARTITION BY service ORDER BY k_stddev_samp_flag_bit1 DESC NULLS FIRST) AS k_stddev_samp_flag_bit1_rank,
        RANK() OVER (PARTITION BY service ORDER BY k_min_status_code_high ASC) AS k_min_status_code_high_rank
    FROM base
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN k_min_status_code_high_plus_k_max_latency_ms >= 81 THEN 'HIGH' ELSE 'LOW' END AS k_min_status_code_high_plus_k_max_latency_ms_tier,
    CASE WHEN k_min_status_code_high_rank >= 9 THEN 'HIGH' ELSE 'LOW' END AS k_min_status_code_high_rank_tier,
    PERCENTILE_APPROX(k_min_status_code_high, 0.95) AS k_min_status_code_high_p95,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(k_min_status_code_high, 0.95) AS k_min_status_code_high_p95_2,
    GROUPING(CASE WHEN k_min_status_code_high_plus_k_max_latency_ms >= 81 THEN 'HIGH' ELSE 'LOW' END) AS gid
FROM enriched
GROUP BY CUBE (CASE WHEN k_min_status_code_high_plus_k_max_latency_ms >= 81 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN k_min_status_code_high_rank >= 9 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY row_count ASC
