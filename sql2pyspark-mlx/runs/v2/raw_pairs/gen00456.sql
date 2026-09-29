SELECT oi.id, oi.unit_price FROM order_items oi WHERE oi.order_id IN (SELECT id FROM orders WHERE amount >= 480) ORDER BY oi.id;
