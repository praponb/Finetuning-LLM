SELECT s.browser, ROUND((100 * SUM(CASE WHEN we.event_type = 'add_to_cart' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_add_to_cart
FROM web_events we
FULL OUTER JOIN sessions s ON we.session_id = s.id
JOIN customers c ON s.customer_id = c.id
WHERE we.event_time < '2025-03-01'
GROUP BY s.browser
