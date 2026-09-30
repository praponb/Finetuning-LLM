SELECT sessions.country, sessions.started_at, sessions.browser, customers.credit_limit, customers.segment, customers.email FROM sessions LEFT JOIN customers ON sessions.customer_id = customers.id
