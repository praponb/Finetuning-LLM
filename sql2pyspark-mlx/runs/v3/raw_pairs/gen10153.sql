SELECT id, device, UPPER(country) AS country_upper FROM sessions WHERE customer_id IS NOT NULL;
