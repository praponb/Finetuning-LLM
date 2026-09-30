SELECT city, TRIM(country) AS clean_country, COALESCE(city, 'unassigned') AS city_filled FROM suppliers
