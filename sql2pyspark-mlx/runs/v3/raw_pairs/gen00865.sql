SELECT s.id, s.start_date FROM subscriptions s WHERE s.start_date > '2023-04-01' AND s.customer_id IN (SELECT id FROM customers WHERE credit_limit >= 8300);
