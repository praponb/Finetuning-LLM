WITH Ranked AS (SELECT *, DENSE_RANK() OVER (PARTITION BY city ORDER BY credit_limit) AS rn FROM customers) SELECT id, city, credit_limit FROM Ranked WHERE rn <= 3
