WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
),
unpacked AS (
    -- Semi-structured parsing
    SELECT
        service,
        host,
        level,
        attr_value,
        CAST((status_code & 1) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(has_fault) OVER (PARTITION BY service) AS service_max_has_fault,
        RANK() OVER (PARTITION BY service ORDER BY has_fault DESC) AS has_fault_rank
    FROM unpacked
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(service_max_has_fault) OVER (PARTITION BY level, service) AS level_min_service_max_has_fault,
        MAX(has_fault) OVER (PARTITION BY service) AS service_max_has_fault_2,
        MIN(has_fault_rank) OVER (PARTITION BY level) AS level_min_has_fault_rank,
        RANK() OVER (PARTITION BY level ORDER BY has_fault NULLS FIRST) AS has_fault_rank_2
    FROM enriched
)
-- Ranking and gap features
SELECT
    *,
    STDDEV_SAMP(service_max_has_fault_2) OVER (PARTITION BY service, host) AS service_stddev_samp_service_max_has_fault_2,
    CUME_DIST() OVER (PARTITION BY host, level ORDER BY level_min_service_max_has_fault DESC) AS level_min_service_max_has_fault_cume_dist,
    ROUND(has_fault_rank_2 / SUM(has_fault_rank_2) OVER (PARTITION BY level), 4) AS has_fault_rank_2_share,
    RANK() OVER (PARTITION BY host ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_rank,
    CUME_DIST() OVER w_hist AS status_code_high_cume_dist
FROM scored
WINDOW w_hist AS (PARTITION BY host ORDER BY status_code_high DESC NULLS FIRST);
