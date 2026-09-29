SELECT s.id, s.monthly_fee FROM subscriptions s WHERE s.start_date >= '2024-09-19' AND s.customer_id IN (SELECT id FROM customers WHERE age < 46) ORDER BY s.id DESC, 2;
