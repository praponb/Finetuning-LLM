SELECT s.customer_id, t.event_type_list, t.product_count
FROM sessions s
JOIN (SELECT session_id, SORT_ARRAY(COLLECT_SET(event_type)) event_type_list, SUM(CASE WHEN page = '/product' THEN 1 ELSE 0 END) product_count
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.product_count > 53
ORDER BY product_count DESC
