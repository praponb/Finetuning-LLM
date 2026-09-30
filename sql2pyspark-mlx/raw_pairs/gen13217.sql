SELECT s.device, SUM(we.duration_sec) AS total_duration_sec, SORT_ARRAY(COLLECT_SET(we.event_type)) AS event_type_list
FROM web_events we
LEFT OUTER JOIN sessions s ON s.id = we.session_id
JOIN customers c ON c.id = s.customer_id
WHERE s.device IN ('desktop', 'mobile')
GROUP BY s.device
ORDER BY device;
