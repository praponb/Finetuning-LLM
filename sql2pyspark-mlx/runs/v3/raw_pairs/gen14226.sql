SELECT sessions.country, customers.city, customers.is_active
FROM sessions
LEFT OUTER JOIN customers ON sessions.customer_id = customers.id;
