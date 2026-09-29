WITH ranked AS (SELECT id, merchant, amount, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount, id) AS rn FROM transactions) SELECT id, merchant, amount FROM ranked WHERE rn <= 3;
