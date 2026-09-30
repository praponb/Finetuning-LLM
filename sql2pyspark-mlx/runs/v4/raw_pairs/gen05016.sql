SELECT customers.segment, accounts.balance, accounts.customer_id, FLOOR(customers.age) age_whole FROM customers LEFT JOIN accounts ON customers.id = accounts.customer_id ORDER BY age_whole;
