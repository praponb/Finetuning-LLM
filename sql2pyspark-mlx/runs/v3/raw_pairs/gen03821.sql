SELECT oi.id, oi.product_id
FROM order_items oi
WHERE oi.order_id IN (SELECT id FROM orders WHERE shipping_fee > 38)
ORDER BY oi.product_id DESC
