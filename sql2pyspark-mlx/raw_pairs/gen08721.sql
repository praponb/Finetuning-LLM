SELECT id, city, name, is_active, UPPER(country) AS country_upper, CONCAT(name, ' ', city) AS name_city FROM customers WHERE LENGTH(email) >= 10
