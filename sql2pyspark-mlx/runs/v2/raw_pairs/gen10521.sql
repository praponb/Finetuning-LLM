SELECT id, customer_id, device, CONCAT_WS('/', browser, device) AS browser_device_key FROM sessions WHERE customer_id IS NOT NULL
