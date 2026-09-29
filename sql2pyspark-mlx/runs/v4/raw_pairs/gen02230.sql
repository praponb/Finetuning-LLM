SELECT o.id, o.order_date FROM orders o WHERE o.customer_id NOT IN (SELECT id FROM customers WHERE city = 'Tokyo') ORDER BY o.id DESC;
