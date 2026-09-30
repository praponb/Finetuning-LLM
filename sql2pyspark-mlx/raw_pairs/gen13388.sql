WITH lines AS (
    SELECT
        id,
        service,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, v
)
SELECT
    id,
    status_code,
    k,
    v,
    LEAD(status_code, 1, 0) OVER (PARTITION BY k ORDER BY status_code, id, k) next_status_code,
    LAG(status_code, 1, 0) OVER (PARTITION BY service ORDER BY status_code, id, k) prev_status_code,
    LAST_VALUE(k, TRUE) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_k,
    NTILE(3) OVER (PARTITION BY service ORDER BY status_code, id, k) status_code_bucket,
    NTILE(5) OVER (PARTITION BY service ORDER BY status_code, id, k) status_code_bucket_2
FROM lines;
