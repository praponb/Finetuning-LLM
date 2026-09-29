SELECT o.id, o.order_date, o.customer_id FROM orders o WHERE o.id IN (SELECT order_id FROM order_items) ORDER BY order_date DESC, id LIMIT 10;
