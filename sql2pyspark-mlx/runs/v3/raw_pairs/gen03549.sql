SELECT id, device, browser, country, TRIM(device) AS clean_device FROM sessions WHERE browser IN ('Chrome', 'Edge');
