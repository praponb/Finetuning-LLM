SELECT country, UPPER(device) device_upper FROM sessions WHERE browser = 'Firefox' AND customer_id IN (16, 28, 39);
