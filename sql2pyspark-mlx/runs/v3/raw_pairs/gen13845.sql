WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit, id) AS pos FROM customers) SELECT * FROM ranked WHERE pos <= 5 ORDER BY country, city
