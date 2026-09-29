WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY k ORDER BY status_code) AS latest_rank
        FROM flattened
    ) t
    WHERE latest_rank = 1
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM ranked
),
islands AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value,
        SUM(is_new_session) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
ranked_app_logs AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY service, k ORDER BY log_ts, id, k) AS rn
        FROM islands
    ) t
    WHERE rn = 1
),
summary AS (
    SELECT
        DATE_TRUNC('HOUR', log_ts) AS hour_window,
        CASE WHEN status_code >= 410 THEN 'LARGE' ELSE 'SMALL' END AS status_code_tier,
        COUNT(*) FILTER (WHERE k = 'version') AS version_rows,
        BIT_OR(status_code) AS status_code_bit_or,
        ROUND(AVG(session_id), 2) AS avg_session_id,
        PERCENTILE_APPROX(session_id, 0.95) AS session_id_p95
    FROM ranked_app_logs
    GROUP BY 1, 2
)
SELECT
    hour_window,
    status_code_tier,
    status_code_bit_or,
    avg_session_id
FROM summary
