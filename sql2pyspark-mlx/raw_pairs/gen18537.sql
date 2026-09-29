SELECT s.plan, s.customer_id, c.segment
FROM subscriptions s
RIGHT OUTER JOIN customers c ON s.customer_id = c.id
WHERE YEAR(s.end_date) = 2025 AND s.customer_id IS NOT NULL
ORDER BY segment;
