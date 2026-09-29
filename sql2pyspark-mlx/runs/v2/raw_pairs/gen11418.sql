SELECT id, customer_id, device, LENGTH(device) AS device_length FROM sessions WHERE customer_id IS NOT NULL AND browser IN ('Firefox', 'Chrome')
