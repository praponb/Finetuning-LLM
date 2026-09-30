WITH flagged AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
islands AS (
    SELECT
        id,
        session_id,
        event_time,
        duration_sec,
        SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    burst_id,
    DATE_TRUNC('YEAR', event_time) AS year_start,
    MAX(event_time) AS ended_at,
    APPROX_PERCENTILE(duration_sec, 0.9) AS duration_sec_p90,
    GROUPING_ID() AS grouping_id
FROM islands
GROUP BY CUBE (burst_id, DATE_TRUNC('YEAR', event_time));
