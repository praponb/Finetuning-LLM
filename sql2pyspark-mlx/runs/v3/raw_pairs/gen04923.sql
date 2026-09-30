SELECT s.id, s.customer_id FROM sessions s WHERE HOUR(s.started_at) BETWEEN 8 AND 11 AND s.customer_id IN (SELECT id FROM customers WHERE city NOT IN ('Chiang Mai', 'New York'));
