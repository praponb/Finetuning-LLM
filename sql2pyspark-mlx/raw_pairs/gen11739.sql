SELECT id, device, customer_id FROM sessions WHERE customer_id = 33 UNION ALL SELECT id, device, customer_id FROM sessions WHERE device = 'mobile';
