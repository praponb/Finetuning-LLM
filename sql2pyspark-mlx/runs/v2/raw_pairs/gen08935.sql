SELECT
    o.status,
    o.customer_id,
    COUNT(*) AS cnt,
    COUNT(s.cost) AS cost_count
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
LEFT OUTER JOIN customers c ON o.customer_id = c.id
WHERE c.age <= 56
GROUP BY o.status, o.customer_id
HAVING COUNT(*) >= 5
