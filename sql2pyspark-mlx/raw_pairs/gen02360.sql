SELECT CASE WHEN s.rating >= 3.6 THEN 'high' WHEN s.rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(i.quantity) AS total_quantity
FROM inventory i
JOIN products p ON p.id = i.product_id
JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 3.6 THEN 'high' WHEN s.rating >= 2.0 THEN 'mid' ELSE 'low' END
ORDER BY 2;
