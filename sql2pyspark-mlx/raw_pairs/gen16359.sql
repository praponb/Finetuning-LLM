SELECT c.id, c.signup_date, c.name FROM customers c WHERE c.segment = 'Corporate' AND NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id) ORDER BY id DESC;
