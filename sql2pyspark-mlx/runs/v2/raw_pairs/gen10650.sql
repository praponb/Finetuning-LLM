SELECT device, customer_id, country, CONCAT(country, ', ', device) AS country_device FROM sessions;
