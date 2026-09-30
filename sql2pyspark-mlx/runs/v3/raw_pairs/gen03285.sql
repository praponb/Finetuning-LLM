WITH session_totals AS (
    SELECT session_id, SUM(CASE WHEN event_type = 'click' THEN duration_sec ELSE 0 END) AS click_duration_sec, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
    FROM web_events
    GROUP BY session_id
)
SELECT s.browser, st.click_duration_sec, st.page_values
FROM sessions s
INNER JOIN session_totals st ON s.id = st.session_id
WHERE st.click_duration_sec > 52
ORDER BY s.browser DESC;
