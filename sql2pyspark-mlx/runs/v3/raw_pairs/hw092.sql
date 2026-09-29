SELECT id, name FROM customers WHERE id IN (SELECT customer_id FROM subscriptions WHERE plan = 'premium');
