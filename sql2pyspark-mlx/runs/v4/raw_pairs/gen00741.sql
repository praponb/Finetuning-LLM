WITH parsed AS (
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        payload_json,
        context,
        SIZE(context) AS num_context,
        context['region'] AS region,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM app_logs
)
SELECT
    CASE WHEN status_code_high >= 3 THEN 'LARGE' ELSE 'SMALL' END AS status_code_high_tier,
    service,
    AVG(latency_ms) AS avg_latency_ms,
    AVG(num_context) FILTER (WHERE service = 'auth') AS auth_avg_num_context,
    COUNT(*) AS num_rows,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list,
    CORR(status_code, latency_ms) AS corr_status_code_latency_ms,
    PERCENTILE_APPROX(latency_ms, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_quartiles
FROM parsed
GROUP BY CASE WHEN status_code_high >= 3 THEN 'LARGE' ELSE 'SMALL' END, service
ORDER BY avg_latency_ms DESC NULLS LAST
