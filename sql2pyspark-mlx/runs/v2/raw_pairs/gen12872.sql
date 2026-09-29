SELECT o.id, o.customer_id FROM orders o WHERE o.customer_id IN (SELECT id FROM customers WHERE signup_date <= '2024-11-01');
