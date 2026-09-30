SELECT id, city, rating, COUNT(*) OVER (PARTITION BY city) AS city_count, FIRST_VALUE(city) OVER (ORDER BY rating, id) AS first_city FROM suppliers WHERE rating >= 3.9
