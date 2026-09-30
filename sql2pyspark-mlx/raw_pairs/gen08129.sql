SELECT c.segment, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(we.page))) AS page_values, SUM(CASE WHEN we.page = '/home' THEN we.duration_sec ELSE 0 END) AS home_duration_sec
FROM web_events we
JOIN sessions s ON we.session_id = s.id
JOIN customers c ON s.customer_id = c.id
GROUP BY c.segment
ORDER BY c.segment;
