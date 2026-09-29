WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
)
SELECT
    service,
    CASE WHEN status_code >= 466 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
    CORR(latency_ms, status_code) AS corr_latency_ms_status_code,
    ROUND(AVG(latency_ms), 2) AS avg_latency_ms,
    COUNT_IF(service = 'search') AS search_events
FROM exploded
GROUP BY service, CASE WHEN status_code >= 466 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING avg_latency_ms < 1000000
ORDER BY corr_latency_ms_status_code NULLS FIRST
