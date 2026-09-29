SELECT s.city, CASE WHEN s.rating >= 3.7 THEN 'high' WHEN s.rating >= 1.6 THEN 'mid' ELSE 'low' END AS rating_bucket, MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM inventory i
FULL OUTER JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE i.store_id IN (2, 5)
GROUP BY s.city, CASE WHEN s.rating >= 3.7 THEN 'high' WHEN s.rating >= 1.6 THEN 'mid' ELSE 'low' END
ORDER BY 1 ASC
