SELECT id, browser, device, country, CONCAT_WS('-', country, device) AS country_device_key, CONCAT(browser, ' - ', country) AS browser_country FROM sessions WHERE HOUR(started_at) BETWEEN 9 AND 15
