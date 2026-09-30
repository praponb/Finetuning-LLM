SELECT sessions.id, sessions.started_at, customers.credit_limit, customers.city FROM sessions FULL OUTER JOIN customers ON sessions.customer_id = customers.id WHERE customers.age BETWEEN 53 AND 57;
