WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
scored AS (
    SELECT
        service,
        log_ts,
        status_code,
        v,
        AVG(latency_ms) OVER (PARTITION BY TO_DATE(log_ts), k) AS log_ts_to_date_avg_latency_ms,
        SUM(latency_ms) OVER (PARTITION BY k) AS k_sum_latency_ms
    FROM flattened
    WHERE k <> 'region'
),
cleaned AS (
    SELECT
        service,
        status_code,
        v,
        CAST(k_sum_latency_ms AS DOUBLE) AS k_sum_latency_ms_str
    FROM scored
    WHERE service <> 'checkout'
),
cleaned_app_logs AS (
    SELECT *, LEAST(status_code, k_sum_latency_ms_str) AS least_status_code_k_sum_latency_ms_str
    FROM cleaned
    WHERE k_sum_latency_ms_str BETWEEN 57 AND 64
)
SELECT
    service,
    CASE WHEN status_code >= 387 THEN 'HIGH' ELSE 'LOW' END AS status_code_tier,
    BIT_OR(status_code) AS status_code_bit_or,
    MIN(least_status_code_k_sum_latency_ms_str) AS min_least_status_code_k_sum_latency_ms_str
FROM cleaned_app_logs
GROUP BY service, CASE WHEN status_code >= 387 THEN 'HIGH' ELSE 'LOW' END
HAVING MIN(least_status_code_k_sum_latency_ms_str) > 0
