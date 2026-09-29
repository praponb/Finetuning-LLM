SELECT s.id, s.start_date, s.end_date FROM subscriptions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE age <= 55) ORDER BY start_date;
