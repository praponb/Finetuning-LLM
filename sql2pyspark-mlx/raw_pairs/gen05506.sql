WITH lines AS (
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, value
),
flattened_attrs AS (
    SELECT
        id,
        service,
        value,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
remaining_keys AS (
    SELECT service
    FROM flattened_attrs
    EXCEPT
    SELECT service
    FROM app_logs
    WHERE level <> 'INFO'
),
kept_rows AS (
    SELECT s.*
    FROM flattened_attrs s
    JOIN remaining_keys k ON s.service = k.service
),
scored AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY service ORDER BY status_code_high DESC NULLS FIRST) AS status_code_high_cume_dist,
        RANK() OVER (ORDER BY status_code_high DESC NULLS FIRST) AS status_code_high_rank,
        MAX(is_flagged) OVER (PARTITION BY service) AS service_max_is_flagged
    FROM kept_rows
    WHERE service = 'auth'
)
SELECT
    service,
    MAX(is_flagged) AS max_is_flagged,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(status_code_high_cume_dist, 0.9) AS status_code_high_cume_dist_p90
FROM scored
GROUP BY service
HAVING status_code_high_cume_dist_p90 > 0
