WITH unpacked AS (
    SELECT
        id,
        service,
        status_code,
        payload_json,
        context['region'] AS region,
        SIZE(context) AS num_context,
        COALESCE(context['trace'], 'unknown') AS trace_or_default
    FROM app_logs
    WHERE service IN ('checkout', 'search')
)
SELECT
    id,
    status_code,
    trace_or_default,
    LAG(num_context, 2) OVER (PARTITION BY service ORDER BY status_code, id) AS prev_num_context,
    SUM(num_context) OVER (PARTITION BY service) AS service_sum_num_context,
    AVG(num_context) OVER (ORDER BY status_code, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS num_context_avg_5_prior,
    LEAD(num_context, 1) OVER (PARTITION BY service ORDER BY status_code, id) AS next_num_context
FROM unpacked
WHERE service NOT IN ('checkout', 'search', 'auth');
