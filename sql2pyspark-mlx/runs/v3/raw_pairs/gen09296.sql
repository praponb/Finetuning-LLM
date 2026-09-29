SELECT customers.id, customers.signup_date, customers.age FROM customers WHERE customers.id IN (SELECT customer_id FROM accounts WHERE customer_id IS NOT NULL) ORDER BY age DESC;
