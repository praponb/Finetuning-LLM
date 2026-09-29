SELECT s.country, COUNT(CASE WHEN we.event_type = 'add_to_cart' THEN 1 END) AS add_to_cart_count, MAX(we.event_time) AS last_event_time
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.credit_limit > 13400
GROUP BY s.country
ORDER BY add_to_cart_count DESC;
