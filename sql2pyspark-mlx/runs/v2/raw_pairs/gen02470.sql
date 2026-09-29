SELECT oi.id, oi.unit_price, oi.order_id
FROM order_items oi
WHERE oi.product_id IS NOT NULL AND oi.order_id IN (SELECT id FROM orders WHERE store_id = 39)
ORDER BY order_id;
