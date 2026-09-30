WITH session_stats AS (
    SELECT session_id, COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.country,
    ss.purchase_count,
    ss.page_values
FROM sessions s
JOIN session_stats ss ON s.id = ss.session_id
WHERE ss.purchase_count >= 6
