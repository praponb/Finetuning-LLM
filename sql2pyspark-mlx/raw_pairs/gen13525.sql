SELECT oi.id, oi.unit_price, oi.quantity, oi.product_id FROM order_items oi WHERE oi.order_id NOT IN (SELECT id FROM orders WHERE amount BETWEEN 1150 AND 1580)
