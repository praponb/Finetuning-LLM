WITH session_totals AS (
    SELECT session_id, AVG(duration_sec) AS avg_duration_sec, SUM(duration_sec) AS total_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.customer_id,
    st.avg_duration_sec,
    st.total_duration_sec
FROM sessions s
INNER JOIN session_totals st ON s.id = st.session_id
WHERE st.total_duration_sec >= 2300;
