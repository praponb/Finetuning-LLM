WITH flattened_attrs AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        payload_json,
        context,
        CAST((status_code & 2) != 0 AS INT) AS has_fault,
        COALESCE(context['trace'], 'unknown') AS trace_or_default,
        context['trace'] AS trace
    FROM app_logs
    WHERE log_ts < '2023-05-01'
)
SELECT
    id,
    service,
    log_ts,
    latency_ms,
    status_code,
    has_fault,
    FIRST_VALUE(latency_ms) IGNORE NULLS OVER (ORDER BY status_code, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_latency_ms,
    NTILE(3) OVER (PARTITION BY level ORDER BY status_code DESC, id) AS status_code_bucket,
    ROW_NUMBER() OVER (PARTITION BY service ORDER BY latency_ms DESC, id) AS seq_no
FROM flattened_attrs
WHERE service IN ('search', 'billing');
