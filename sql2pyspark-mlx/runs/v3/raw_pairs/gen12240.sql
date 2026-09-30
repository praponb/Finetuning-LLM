SELECT
    CASE WHEN s.rating >= 4.4 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket,
    s.city,
    AVG(i.quantity) AS avg_quantity,
    SUM(i.quantity) AS total_quantity
FROM inventory i
LEFT JOIN products p ON p.id = i.product_id
FULL OUTER JOIN suppliers s ON s.id = p.supplier_id
GROUP BY CASE WHEN s.rating >= 4.4 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END, s.city
ORDER BY city DESC
