SELECT id, device, CONCAT_WS(',', browser, device) AS browser_device_key, CASE WHEN device = 'desktop' THEN 1 ELSE 0 END AS is_desktop FROM sessions WHERE country IN ('TH', 'SG', 'JP')
