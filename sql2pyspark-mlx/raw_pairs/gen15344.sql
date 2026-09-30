WITH flattened_attrs AS (
    SELECT
        id,
        service,
        log_ts,
        status_code,
        context,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        context['trace'] AS trace,
        COALESCE(context['trace'], 'direct') AS trace_or_default,
        CAST((status_code & 8) != 0 AS INT) AS has_fault
    FROM app_logs
)
SELECT
    id,
    service,
    status_code,
    trace,
    trace_or_default,
    has_fault,
    AVG(status_code) OVER w_part AS status_code_avg_5_prior,
    LAST_VALUE(trace) IGNORE NULLS OVER (PARTITION BY service ORDER BY status_code, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_trace,
    ROW_NUMBER() OVER w_hist AS seq_no
FROM flattened_attrs
WHERE status_code > 386
WINDOW w_part AS (ORDER BY status_code, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_hist AS (PARTITION BY service ORDER BY has_fault DESC, id);
