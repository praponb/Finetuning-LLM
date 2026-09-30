WITH numbered AS (SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rn FROM suppliers) SELECT id, city, rating FROM numbered WHERE rn <= 2 ORDER BY id DESC;
