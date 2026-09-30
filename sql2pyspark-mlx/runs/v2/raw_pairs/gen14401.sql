SELECT sessions.id, sessions.country, sessions.device
FROM sessions
WHERE sessions.customer_id IS NULL AND sessions.customer_id IN (SELECT id FROM customers WHERE country = 'US')
