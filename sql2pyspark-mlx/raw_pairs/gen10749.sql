WITH unpacked AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        context,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count
    FROM app_logs
    WHERE service != 'search'
)
SELECT
    *,
    SUM(latency_ms) OVER w_running AS log_ts_year_sum_latency_ms,
    MAX(status_code) OVER (PARTITION BY service) AS service_max_status_code
FROM unpacked
WINDOW w_running AS (PARTITION BY YEAR(log_ts))
