WITH ranked AS (SELECT id, method, amount, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS rn FROM payments) SELECT * FROM ranked WHERE rn <= 3 ORDER BY amount DESC;
