WITH session_totals AS (
    SELECT session_id, SUM(CASE WHEN event_type = 'click' THEN duration_sec ELSE 0 END) AS click_duration_sec, AVG(duration_sec) AS avg_duration_sec
    FROM web_events
    WHERE event_time < '2024-02-11'
    GROUP BY session_id
)
SELECT
    s.id,
    s.started_at,
    s.customer_id,
    st.click_duration_sec,
    st.avg_duration_sec
FROM sessions s
LEFT JOIN session_totals st ON s.id = st.session_id;
