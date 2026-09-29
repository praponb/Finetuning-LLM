WITH session_totals AS (
    SELECT session_id, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT s.id, s.country, st.stddev_duration_sec
FROM sessions s
JOIN session_totals st ON s.id = st.session_id
ORDER BY s.id DESC, country DESC
LIMIT 5;
