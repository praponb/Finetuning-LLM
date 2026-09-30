SELECT s.id, s.customer_id, s.start_date FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE age < 43) ORDER BY s.customer_id DESC;
