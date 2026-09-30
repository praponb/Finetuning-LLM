SELECT p.brand, CASE WHEN p.cost >= 750 THEN 'high' WHEN p.cost >= 140 THEN 'mid' ELSE 'low' END cost_bucket, MAX(oi.quantity) max_quantity
FROM order_items oi
INNER JOIN products p ON p.id = oi.product_id
INNER JOIN orders o ON oi.order_id = o.id
WHERE o.store_id IS NULL
GROUP BY p.brand, CASE WHEN p.cost >= 750 THEN 'high' WHEN p.cost >= 140 THEN 'mid' ELSE 'low' END
ORDER BY cost_bucket DESC
