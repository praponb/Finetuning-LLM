SELECT id, city, rating FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS rn FROM suppliers) AS r WHERE rn <= 1 ORDER BY rating DESC;
