SELECT id, COALESCE(city, country, 'unknown') AS location FROM customers;
