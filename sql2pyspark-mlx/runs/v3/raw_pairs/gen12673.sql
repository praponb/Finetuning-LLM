SELECT id, city, rating FROM (SELECT id, city, rating, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS rn FROM suppliers) AS t WHERE rn <= 5 ORDER BY id DESC, rating DESC;
