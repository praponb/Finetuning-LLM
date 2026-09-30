SELECT id, name, SUBSTRING(city, 1, 3) AS city_prefix FROM customers ORDER BY name;
