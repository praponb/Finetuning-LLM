SELECT s.id, s.country, s.started_at, s.browser FROM sessions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE YEAR(signup_date) >= 2024)
