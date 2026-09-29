SELECT id, browser, country FROM sessions WHERE country = 'TH' UNION SELECT id, browser, country FROM sessions WHERE customer_id = 16 ORDER BY country;
