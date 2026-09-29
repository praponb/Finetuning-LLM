SELECT orders.id, orders.customer_id FROM orders WHERE orders.discount <= 0.28 AND orders.customer_id IN (SELECT id FROM customers WHERE signup_date <= '2023-03-25')
