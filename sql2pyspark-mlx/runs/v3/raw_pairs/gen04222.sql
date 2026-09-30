SELECT id, country, rating FROM (SELECT id, country, rating, DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rn FROM suppliers) AS ranked WHERE rn = 1
