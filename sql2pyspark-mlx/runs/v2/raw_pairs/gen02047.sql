SELECT CASE WHEN s.rating >= 3.9 THEN 'high' WHEN s.rating >= 1.9 THEN 'mid' ELSE 'low' END rating_bucket, MIN(i.last_restock) first_last_restock, MAX(i.quantity) - MIN(i.quantity) quantity_range
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON s.id = p.supplier_id
GROUP BY CASE WHEN s.rating >= 3.9 THEN 'high' WHEN s.rating >= 1.9 THEN 'mid' ELSE 'low' END
HAVING MAX(i.quantity) - MIN(i.quantity) > 21
ORDER BY rating_bucket
