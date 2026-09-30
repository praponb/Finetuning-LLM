WITH latest_match AS (
    SELECT
        s.id,
        s.customer_id,
        s.device,
        s.browser,
        s.started_at,
        latest.web_event_event_type,
        latest.event_time
    FROM sessions s
    JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type, c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
prev_events AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY web_event_event_type ORDER BY event_time, id) AS prev_event_time
    FROM latest_match
),
gaps AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY web_event_event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
gaps_2 AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY customer_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY customer_id ORDER BY event_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session_2
    FROM islands
),
sessionized AS (
    SELECT *, SUM(is_new_session_2) OVER (PARTITION BY customer_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps_2
),
with_dims AS (
    SELECT
        s.*,
        b.age,
        b.email,
        b.segment
    FROM sessionized s
    INNER JOIN customers b
        ON s.customer_id = b.id
)
SELECT
    DATE_TRUNC('QUARTER', prev_event_time) AS quarter_start,
    CASE WHEN burst_id >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS burst_id_tier,
    DATE_TRUNC('YEAR', started_at) AS year_start,
    SUM(CASE WHEN device = 'desktop' THEN is_new_session_2 ELSE 0 END) AS desktop_is_new_session_2,
    MAX(event_time) AS ended_at,
    COUNT(CASE WHEN segment = 'SMB' THEN 1 END) AS smb_count,
    AVG(is_new_session_2) AS avg_is_new_session_2,
    COUNT(*) FILTER (WHERE device = 'desktop') AS desktop_rows,
    GROUPING(DATE_TRUNC('QUARTER', prev_event_time)) AS grouping_level
FROM with_dims
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', prev_event_time), CASE WHEN burst_id >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('YEAR', started_at)), (DATE_TRUNC('QUARTER', prev_event_time), CASE WHEN burst_id >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY avg_is_new_session_2
