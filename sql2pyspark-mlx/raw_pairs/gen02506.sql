SELECT
    c.city,
    COUNT(o.id) order_count,
    ROUND((100 * SUM(CASE WHEN o.status = 'RETURNED' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_returned,
    COUNT(DISTINCT o.amount) unique_stores
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id
GROUP BY c.city
HAVING COUNT(o.id) >= 1
ORDER BY city DESC, unique_stores DESC
