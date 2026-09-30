SELECT c.id, c.country, c.email, c.signup_date FROM customers c WHERE NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id) ORDER BY 3 ASC, id ASC LIMIT 50;
