SELECT sessions.started_at, customers.credit_limit FROM sessions LEFT OUTER JOIN customers ON sessions.customer_id = customers.id ORDER BY started_at DESC
