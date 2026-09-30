SELECT CASE WHEN s.rating >= 3.7 THEN 'high' WHEN s.rating >= 2.2 THEN 'mid' ELSE 'low' END AS rating_bucket, MIN(i.last_restock) AS first_last_restock
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON s.id = p.supplier_id
GROUP BY CASE WHEN s.rating >= 3.7 THEN 'high' WHEN s.rating >= 2.2 THEN 'mid' ELSE 'low' END
ORDER BY rating_bucket;
