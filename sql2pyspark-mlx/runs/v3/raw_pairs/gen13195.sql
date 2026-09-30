SELECT id, customer_id, TRIM(device) AS clean_device, CONCAT_WS('-', browser, device) AS browser_device_key FROM sessions ORDER BY customer_id DESC
