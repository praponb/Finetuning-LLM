SELECT * FROM (SELECT id, country, rating, city, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS rn FROM suppliers) AS r WHERE rn <= 1;
