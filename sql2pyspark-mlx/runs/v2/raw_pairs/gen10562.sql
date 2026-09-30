SELECT id, is_active, email, SUBSTRING(country, 1, 3) AS country_prefix FROM customers ORDER BY id DESC;
