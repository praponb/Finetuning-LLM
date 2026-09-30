SELECT customer_id, order_date FROM orders WHERE customer_id IS NOT NULL UNION SELECT customer_id, order_date FROM orders WHERE status != 'RETURNED';
