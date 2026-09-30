SELECT s.id, s.browser, s.customer_id FROM sessions s WHERE s.started_at < '2023-04-01' AND s.customer_id IN (SELECT id FROM customers WHERE segment IN ('Consumer', 'SMB'));
