SELECT o.id, o.order_date FROM orders o WHERE o.status <> 'PENDING' AND EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id) ORDER BY order_date;
