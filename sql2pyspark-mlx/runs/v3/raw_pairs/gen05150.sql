SELECT id, device, browser FROM sessions WHERE customer_id IN (16, 21) UNION ALL SELECT id, device, browser FROM sessions WHERE browser <> 'Edge';
