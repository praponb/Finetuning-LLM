SELECT sessions.id, sessions.browser FROM sessions WHERE sessions.customer_id IN (SELECT id FROM customers WHERE country IN ('JP', 'SG'));
