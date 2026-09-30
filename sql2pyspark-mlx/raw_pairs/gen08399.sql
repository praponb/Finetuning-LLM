WITH flattened AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
)
SELECT
    host,
    BIT_AND(latency_ms) AS latency_ms_bit_and,
    PERCENTILE_APPROX(status_code, 0.9) AS status_code_p90
FROM flattened
GROUP BY host
HAVING status_code_p90 > 0
ORDER BY status_code_p90 DESC
