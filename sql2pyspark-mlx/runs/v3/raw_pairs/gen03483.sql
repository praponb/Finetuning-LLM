WITH session_totals AS (
    SELECT session_id, MAX(event_time) AS last_event_time
    FROM web_events
    GROUP BY session_id
)
SELECT s.started_at, st.last_event_time
FROM sessions s
LEFT JOIN session_totals st ON s.id = st.session_id
ORDER BY started_at, st.last_event_time;
