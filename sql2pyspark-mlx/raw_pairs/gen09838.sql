WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, value
),
enriched AS (
    SELECT
        *,
        COUNT(status_code) OVER (PARTITION BY host ORDER BY log_ts, id, key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_status_code,
        MAX(status_code) OVER (PARTITION BY host ORDER BY log_ts, id, key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_status_code,
        MIN(status_code) OVER (PARTITION BY host ORDER BY log_ts, id, key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_status_code
    FROM flattened
),
gaps AS (
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, key) + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM enriched
),
sessions_app_logs AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        key,
        value,
        last3_max_status_code,
        SUM(new_group_flag) OVER (PARTITION BY host ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY service, key ORDER BY last3_max_status_code DESC NULLS FIRST, id, key) AS rank_in_group
    FROM sessions_app_logs
    QUALIFY rank_in_group = 1
),
gaps_2 AS (
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, key) + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM deduped
),
islands AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        key,
        SUM(session_start_flag) OVER (PARTITION BY service ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps_2
)
SELECT
    CASE WHEN session_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS session_id_tier,
    PERCENTILE_APPROX(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles,
    MIN(session_id) AS min_session_id
FROM islands
GROUP BY CASE WHEN session_id >= 8 THEN 'LARGE' ELSE 'SMALL' END
