WITH parsed AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        payload_json,
        context,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        SIZE(context) AS num_context
    FROM app_logs
)
SELECT
    id,
    latency_ms,
    status_code_high,
    user_tier,
    SUM(num_context) OVER (PARTITION BY TO_DATE(log_ts) ORDER BY log_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_num_context,
    AVG(latency_ms) OVER (ORDER BY log_ts, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) latency_ms_avg_5_prior,
    NTILE(4) OVER w_hist status_code_high_bucket,
    NTILE(4) OVER (ORDER BY status_code_high DESC, id) status_code_high_bucket_2
FROM parsed
WHERE status_code_high >= 1
WINDOW w_hist AS (ORDER BY status_code_high, id)
