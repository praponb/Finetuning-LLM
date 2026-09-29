SELECT sessions.id, sessions.customer_id, customers.signup_date, customers.city, customers.is_active FROM sessions LEFT JOIN customers ON sessions.customer_id = customers.id ORDER BY id DESC;
