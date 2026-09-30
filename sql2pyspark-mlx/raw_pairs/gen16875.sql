SELECT CASE WHEN s.rating >= 4.2 THEN 'high' WHEN s.rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket, s.city, MAX(i.last_restock) AS last_last_restock
FROM inventory i
JOIN products p ON p.id = i.product_id
JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 4.2 THEN 'high' WHEN s.rating >= 1.5 THEN 'mid' ELSE 'low' END, s.city
ORDER BY 1, city
LIMIT 5
