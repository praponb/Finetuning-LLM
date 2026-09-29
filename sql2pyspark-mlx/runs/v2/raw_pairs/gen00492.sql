SELECT oi.id, oi.unit_price, oi.quantity FROM order_items oi WHERE oi.unit_price >= 240 AND oi.order_id IN (SELECT id FROM orders WHERE amount BETWEEN 740 AND 1280)
