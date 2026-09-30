SELECT oi.id, oi.unit_price, oi.order_id FROM order_items oi WHERE oi.product_id NOT IN (SELECT id FROM products WHERE cost < 450)
