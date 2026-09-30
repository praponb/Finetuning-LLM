SELECT * FROM (SELECT id, city, rating, RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rn FROM suppliers) AS t WHERE rn <= 3 ORDER BY rating
