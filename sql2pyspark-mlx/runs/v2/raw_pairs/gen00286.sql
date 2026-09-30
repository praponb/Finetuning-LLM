SELECT c.country, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(we.event_type))) AS event_type_values
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY c.country
ORDER BY event_type_values, country
LIMIT 1;
