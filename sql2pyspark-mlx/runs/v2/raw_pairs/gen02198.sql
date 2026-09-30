WITH numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) rn FROM suppliers) SELECT id, country, rating FROM numbered WHERE rn <= 1 ORDER BY id DESC, 3;
