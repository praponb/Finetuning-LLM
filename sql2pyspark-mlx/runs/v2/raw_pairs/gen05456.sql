SELECT s.customer_id, s.browser, c.email, c.signup_date, c.city FROM sessions s INNER JOIN customers c ON c.id = s.customer_id WHERE s.device = 'mobile';
