WITH session_totals AS (
    SELECT session_id, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT s.customer_id, s.browser, st.avg_duration_sec
FROM sessions s
LEFT OUTER JOIN session_totals st ON s.id = st.session_id
ORDER BY s.customer_id DESC
