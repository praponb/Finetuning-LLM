SELECT id, customer_id, browser, COALESCE(country, 'Unknown') country_filled FROM sessions ORDER BY country_filled ASC;
