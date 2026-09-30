SELECT oi.id, oi.product_id, oi.unit_price
FROM order_items oi
WHERE oi.quantity > 3 AND oi.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee > 29)
ORDER BY id, product_id;
