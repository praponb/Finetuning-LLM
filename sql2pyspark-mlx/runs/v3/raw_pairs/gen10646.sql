SELECT sessions.id, sessions.country FROM sessions WHERE sessions.customer_id IN (SELECT id FROM customers WHERE city = 'Bangkok')
