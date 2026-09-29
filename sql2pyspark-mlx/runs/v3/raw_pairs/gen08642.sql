WITH session_totals AS (
    SELECT session_id, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
    FROM web_events
    WHERE duration_sec BETWEEN 296 AND 365
    GROUP BY session_id
)
SELECT
    s.customer_id,
    s.started_at,
    st.duration_sec_range,
    st.page_values
FROM sessions s
INNER JOIN session_totals st ON s.id = st.session_id
WHERE st.duration_sec_range > 72
