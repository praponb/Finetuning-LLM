SELECT
    s.id,
    COUNT(o.id) AS order_count,
    SUM(o.discount) AS total_discount,
    SUM(CASE WHEN o.status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_count
FROM stores s
LEFT JOIN orders o ON s.id = o.store_id
WHERE s.store_name LIKE 'A%'
GROUP BY s.id
HAVING SUM(o.discount) >= 2.0
ORDER BY order_count ASC;
