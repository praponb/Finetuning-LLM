SELECT oi.id, oi.unit_price, oi.quantity, oi.product_id FROM order_items oi WHERE oi.quantity <> 5 AND oi.order_id NOT IN (SELECT id FROM orders WHERE customer_id = 29) ORDER BY unit_price DESC
