SELECT s.id, s.started_at FROM sessions s WHERE s.customer_id IN (6, 24) AND s.customer_id IN (SELECT id FROM customers WHERE segment = 'Corporate') ORDER BY started_at;
