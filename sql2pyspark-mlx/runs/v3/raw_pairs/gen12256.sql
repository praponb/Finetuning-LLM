SELECT id, rating, city, country FROM suppliers WHERE NOT (city = 'New York' AND (rating <= 2.3 OR country IN ('SG', 'DE')));
