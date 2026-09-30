SELECT id, customer_id, browser, started_at, CONCAT_WS('-', device, country) device_country_key FROM sessions WHERE customer_id IS NOT NULL
