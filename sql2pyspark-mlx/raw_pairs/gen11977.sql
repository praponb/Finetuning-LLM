SELECT CASE WHEN o.shipping_fee >= 30 THEN 'high' WHEN o.shipping_fee >= 7 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket, MAX(oi.quantity) AS max_quantity, MAX(oi.quantity) - MIN(oi.quantity) AS quantity_range
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
WHERE oi.order_id IN (8, 28, 32, 36)
GROUP BY CASE WHEN o.shipping_fee >= 30 THEN 'high' WHEN o.shipping_fee >= 7 THEN 'mid' ELSE 'low' END
