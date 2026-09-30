SELECT s.id, s.status, s.start_date, s.end_date FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE LENGTH(email) > 8) ORDER BY 1 LIMIT 5;
