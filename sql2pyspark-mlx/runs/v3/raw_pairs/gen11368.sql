SELECT id, customer_id, started_at, CONCAT_WS('-', browser, country) browser_country_key FROM sessions WHERE customer_id = 4 ORDER BY id, started_at;
