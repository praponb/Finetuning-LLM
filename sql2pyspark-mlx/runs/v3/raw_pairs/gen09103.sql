SELECT s.id, s.device, s.customer_id FROM sessions s WHERE s.customer_id IS NOT NULL AND s.customer_id IN (SELECT id FROM customers WHERE segment = 'Corporate') ORDER BY device;
