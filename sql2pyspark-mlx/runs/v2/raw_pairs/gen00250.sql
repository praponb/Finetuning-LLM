WITH session_totals AS (
    SELECT session_id, MIN(event_time) AS min_event_time, COUNT(CASE WHEN page = '/cart' THEN 1 END) AS cart_count
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.device,
    st.min_event_time,
    st.cart_count
FROM sessions s
INNER JOIN session_totals st ON s.id = st.session_id
