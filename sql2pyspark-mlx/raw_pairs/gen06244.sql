SELECT customers.id, customers.country, customers.email FROM customers WHERE customers.age < 67 AND customers.id IN (SELECT customer_id FROM subscriptions WHERE status = 'PAUSED')
