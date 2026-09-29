SELECT c.segment, c.signup_date, c.is_active, s.customer_id, s.browser FROM customers c LEFT JOIN sessions s ON c.id = s.customer_id WHERE c.is_active ORDER BY c.segment;
