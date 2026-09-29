SELECT id, started_at, country, device, LENGTH(device) AS device_length FROM sessions WHERE customer_id IS NOT NULL ORDER BY started_at DESC, device_length DESC;
