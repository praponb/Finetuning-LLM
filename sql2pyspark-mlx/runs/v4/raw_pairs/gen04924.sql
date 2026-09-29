SELECT city, COALESCE(city, 'N/A') AS city_filled
FROM suppliers
