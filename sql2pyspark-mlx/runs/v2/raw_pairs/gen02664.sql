SELECT
    s.city,
    COUNT(o.id) order_count,
    ROUND((100 * SUM(CASE WHEN o.status = 'CANCELLED' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_cancelled,
    AVG(o.amount) avg_amount
FROM stores s
JOIN orders o ON o.store_id = s.id
WHERE s.opened_date BETWEEN '2023-11-16' AND '2025-09-01'
GROUP BY s.city
ORDER BY pct_cancelled DESC;
