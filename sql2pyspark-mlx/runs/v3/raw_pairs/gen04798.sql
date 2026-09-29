SELECT id, started_at, country, browser, CONCAT(browser, '/', device) browser_device FROM sessions WHERE browser IN ('Edge', 'Firefox', 'Safari') ORDER BY browser_device DESC, id LIMIT 5
