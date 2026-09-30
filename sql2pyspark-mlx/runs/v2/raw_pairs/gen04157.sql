SELECT customer_id, device, started_at, CONCAT(browser, ' - ', country) browser_country FROM sessions
