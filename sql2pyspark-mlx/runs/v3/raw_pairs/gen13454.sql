SELECT s.browser, s.country, s.customer_id, c.segment, c.signup_date FROM sessions s FULL OUTER JOIN customers c ON s.customer_id = c.id
