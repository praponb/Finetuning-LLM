WITH session_totals AS (
    SELECT session_id, MAX(duration_sec) AS max_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT s.started_at, st.max_duration_sec
FROM sessions s
LEFT JOIN session_totals st ON s.id = st.session_id
ORDER BY s.started_at
