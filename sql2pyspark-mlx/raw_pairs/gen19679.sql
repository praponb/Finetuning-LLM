SELECT id, device, TRIM(device) AS clean_device, CONCAT(device, ' ', country) AS device_country FROM sessions WHERE device IN ('tablet', 'mobile', 'desktop')
