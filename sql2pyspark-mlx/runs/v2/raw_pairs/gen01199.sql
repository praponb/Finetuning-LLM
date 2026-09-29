SELECT id, city, rating, country, FIRST_VALUE(country) OVER (PARTITION BY city ORDER BY rating, id) AS first_country FROM suppliers;
