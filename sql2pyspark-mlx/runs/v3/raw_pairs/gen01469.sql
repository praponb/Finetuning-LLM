SELECT CASE WHEN o.shipping_fee >= 36 THEN 'high' WHEN o.shipping_fee >= 5 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket, o.customer_id, MAX(oi.unit_price) AS max_unit_price
FROM order_items oi
LEFT JOIN orders o ON o.id = oi.order_id
WHERE o.store_id IN (7, 32, 38)
GROUP BY CASE WHEN o.shipping_fee >= 36 THEN 'high' WHEN o.shipping_fee >= 5 THEN 'mid' ELSE 'low' END, o.customer_id;
