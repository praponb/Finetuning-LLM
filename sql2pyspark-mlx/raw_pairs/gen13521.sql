SELECT CASE WHEN s.rating >= 3.6 THEN 'high' WHEN s.rating >= 1.8 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(oi.unit_price) AS total_unit_price, MAX(oi.quantity) - MIN(oi.quantity) AS quantity_range
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE oi.quantity < 6
GROUP BY CASE WHEN s.rating >= 3.6 THEN 'high' WHEN s.rating >= 1.8 THEN 'mid' ELSE 'low' END
ORDER BY rating_bucket;
