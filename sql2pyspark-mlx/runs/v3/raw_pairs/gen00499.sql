SELECT city, CEIL(rating) AS rating_whole FROM suppliers WHERE city = 'Chiang Mai' AND (city IS NOT NULL OR city = 'Tokyo');
