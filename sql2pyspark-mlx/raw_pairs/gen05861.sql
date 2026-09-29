SELECT s.id, s.browser FROM sessions s WHERE TO_DATE(s.started_at) < '2023-04-01' AND s.customer_id IN (SELECT id FROM customers WHERE city = 'London');
