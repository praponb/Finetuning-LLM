SELECT id, credit_limit, name, email, LOWER(country) AS country_lower, LENGTH(country) AS country_length FROM customers WHERE segment = 'Consumer';
