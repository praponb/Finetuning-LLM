SELECT s.id, s.monthly_fee, s.status, s.end_date FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE age BETWEEN 31 AND 40) ORDER BY s.id DESC LIMIT 1
