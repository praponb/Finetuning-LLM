WITH flattened AS (
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, value
)
SELECT
    *,
    CORR(latency_ms, status_code) OVER w_running AS corr_latency_ms_status_code,
    AVG(status_code) OVER w_running_2 AS status_code_avg_2_prior,
    LAST_VALUE(k, TRUE) OVER (ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_k
FROM flattened
WINDOW w_running AS (PARTITION BY k, host), w_running_2 AS (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING);
