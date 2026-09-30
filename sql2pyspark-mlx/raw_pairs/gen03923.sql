WITH parsed AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        payload_json,
        context['trace'] AS trace,
        SIZE(context) AS num_context,
        GET_JSON_OBJECT(payload_json, '$.user.id') AS user_id,
        context['version'] AS version,
        ARRAY_CONTAINS(MAP_KEYS(context), 'version') AS has_version
    FROM app_logs
)
SELECT
    id,
    has_version,
    PERCENT_RANK() OVER (PARTITION BY host, service ORDER BY num_context DESC) AS num_context_percent_rank,
    AVG(num_context) OVER w_grp AS num_context_avg_30m
FROM parsed
WHERE log_ts < '2025-02-04'
WINDOW w_grp AS (PARTITION BY YEAR(log_ts), host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
