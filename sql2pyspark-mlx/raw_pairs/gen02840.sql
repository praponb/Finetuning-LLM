WITH numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY amount) AS rn FROM payments) SELECT * FROM numbered WHERE rn <= 3;
