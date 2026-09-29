WITH session_totals AS (
    SELECT session_id, SUM(CASE WHEN page = '/search' THEN 1 ELSE 0 END) AS search_count, SUM(CASE WHEN page = '/checkout' THEN 1 ELSE 0 END) AS checkout_count
    FROM web_events
    WHERE event_time >= '2023-12-01'
    GROUP BY session_id
)
SELECT
    s.country,
    s.customer_id,
    st.search_count,
    st.checkout_count
FROM sessions s
LEFT JOIN session_totals st ON s.id = st.session_id
ORDER BY customer_id DESC
