SELECT c.signup_date, c.email, c.credit_limit, s.customer_id, s.end_date FROM customers c LEFT OUTER JOIN subscriptions s ON c.id = s.customer_id WHERE c.name IS NULL
