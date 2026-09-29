WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit, id) AS rn FROM customers) SELECT * FROM numbered WHERE rn <= 3
