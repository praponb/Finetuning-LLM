SELECT id, signup_date, LENGTH(country) AS country_length FROM customers WHERE country = 'DE' OR city = 'Chiang Mai';
