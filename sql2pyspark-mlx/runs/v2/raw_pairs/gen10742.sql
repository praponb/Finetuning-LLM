SELECT *, SUBSTRING(device, 1, 3) AS device_prefix FROM sessions WHERE device = 'tablet' AND customer_id IS NULL;
