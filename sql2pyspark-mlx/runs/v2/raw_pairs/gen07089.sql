SELECT id, rating, country, COALESCE(city, 'Unknown') AS city_filled FROM suppliers WHERE rating BETWEEN 3.5 AND 4.1 OR city IS NOT NULL;
