WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost, id) AS rn FROM products) SELECT id, category, cost FROM ranked WHERE rn = 1;
