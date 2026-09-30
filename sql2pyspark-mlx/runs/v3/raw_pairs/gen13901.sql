SELECT customers.id, customers.email
FROM customers
WHERE NOT customers.is_active AND customers.id IN (SELECT customer_id FROM subscriptions);
