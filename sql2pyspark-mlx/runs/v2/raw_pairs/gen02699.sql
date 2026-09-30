WITH session_totals AS (
    SELECT session_id, COUNT(duration_sec) AS duration_sec_count
    FROM web_events
    WHERE session_id IN (19, 28)
    GROUP BY session_id
)
SELECT
    s.id,
    s.browser,
    s.customer_id,
    st.duration_sec_count
FROM sessions s
JOIN session_totals st ON s.id = st.session_id
