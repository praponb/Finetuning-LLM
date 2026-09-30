SELECT s.device, COUNT(we.id) web_event_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(we.page))) page_values
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
WHERE s.customer_id = 3
GROUP BY s.device
ORDER BY device ASC;
