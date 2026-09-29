SELECT id, browser, customer_id, UPPER(device) AS device_upper FROM sessions WHERE customer_id IS NOT NULL;
