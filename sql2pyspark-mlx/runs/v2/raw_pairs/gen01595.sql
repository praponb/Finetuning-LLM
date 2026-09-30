SELECT id, customer_id, country, browser, CONCAT_WS('-', country, browser) AS country_browser_key FROM sessions WHERE customer_id IN (7, 12, 19);
