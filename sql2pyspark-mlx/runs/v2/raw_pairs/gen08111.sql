SELECT sessions.id, sessions.started_at, sessions.country, customers.segment, customers.is_active, customers.signup_date FROM sessions LEFT JOIN customers ON sessions.customer_id = customers.id;
