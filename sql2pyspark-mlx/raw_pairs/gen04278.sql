SELECT s.id, s.plan, s.start_date FROM subscriptions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE name LIKE '%Garc%');
