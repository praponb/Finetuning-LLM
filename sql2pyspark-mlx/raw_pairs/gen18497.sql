WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
),
flattened_attrs AS (
    SELECT
        service,
        host,
        latency_ms,
        status_code,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS has_fault
    FROM exploded
),
kept_keys AS (
    SELECT service
    FROM flattened_attrs
    INTERSECT ALL
    SELECT service
    FROM app_logs
    WHERE latency_ms = 677
),
verified AS (
    SELECT s.*
    FROM flattened_attrs s
    INNER JOIN kept_keys k ON s.service = k.service
),
with_windows AS (
    SELECT
        *,
        STDDEV_SAMP(status_code) OVER (PARTITION BY host, service) host_stddev_samp_status_code,
        PERCENT_RANK() OVER (PARTITION BY service ORDER BY has_fault ASC NULLS FIRST) has_fault_percent_rank,
        RANK() OVER (PARTITION BY service, host ORDER BY status_code DESC) status_code_rank,
        CUME_DIST() OVER (PARTITION BY host ORDER BY has_fault ASC NULLS FIRST) has_fault_cume_dist
    FROM verified
    WHERE status_code_high > 1
),
valid AS (
    SELECT *, FLOOR(latency_ms) latency_ms_whole
    FROM with_windows
    WHERE latency_ms BETWEEN 1750 AND 2087
)
SELECT
    service,
    kpi,
    reading
FROM (
    SELECT
        service,
        has_fault_percent_rank,
        has_fault_cume_dist
    FROM valid
) src
UNPIVOT (
    reading FOR kpi IN (has_fault_percent_rank, has_fault_cume_dist AS HAS_FAULT_CUME_DIST_VAL)
) u;
