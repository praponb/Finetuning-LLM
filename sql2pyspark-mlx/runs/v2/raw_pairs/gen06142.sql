SELECT orders.id, orders.order_date FROM orders WHERE orders.amount < 990 AND orders.customer_id NOT IN (SELECT id FROM customers WHERE is_active = FALSE);
