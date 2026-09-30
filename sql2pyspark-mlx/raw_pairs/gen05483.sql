SELECT CASE WHEN s.rating >= 4.0 THEN 'high' WHEN s.rating >= 1.4 THEN 'mid' ELSE 'low' END AS rating_bucket, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity, AVG(i.quantity) AS avg_quantity
FROM inventory i
FULL OUTER JOIN products p ON i.product_id = p.id
JOIN suppliers s ON s.id = p.supplier_id
WHERE s.supplier_name IS NOT NULL
GROUP BY CASE WHEN s.rating >= 4.0 THEN 'high' WHEN s.rating >= 1.4 THEN 'mid' ELSE 'low' END
ORDER BY avg_quantity ASC
