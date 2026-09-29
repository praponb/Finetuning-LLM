SELECT sessions.id, sessions.device
FROM sessions
WHERE sessions.customer_id IN (SELECT id FROM customers WHERE country = 'SG');
