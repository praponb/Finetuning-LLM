WITH session_totals AS (
    SELECT session_id, MAX(event_time) last_event_time
    FROM web_events
    WHERE session_id = 27
    GROUP BY session_id
)
SELECT s.browser, st.last_event_time
FROM sessions s
LEFT JOIN session_totals st ON s.id = st.session_id
ORDER BY browser
