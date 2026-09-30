WITH ranked AS (SELECT id, status, amount, method, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) AS pos FROM payments) SELECT * FROM ranked WHERE pos <= 5 ORDER BY 4 ASC
