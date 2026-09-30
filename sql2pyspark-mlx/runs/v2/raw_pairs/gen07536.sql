SELECT id, browser, customer_id, country, TRIM(device) clean_device, TO_DATE(started_at) started_at_date FROM sessions ORDER BY id ASC
