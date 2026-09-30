SELECT id, city, rating FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rn FROM suppliers) AS t WHERE rn <= 2 ORDER BY 2 DESC;
