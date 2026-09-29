SELECT sessions.id, sessions.country, sessions.device, sessions.customer_id FROM sessions WHERE sessions.customer_id NOT IN (SELECT id FROM customers WHERE country = 'JP');
