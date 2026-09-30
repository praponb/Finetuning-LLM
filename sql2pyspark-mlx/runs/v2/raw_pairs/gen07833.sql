WITH ranked AS (SELECT id, brand, cost, RANK() OVER (PARTITION BY brand ORDER BY cost DESC) AS rn FROM products) SELECT id, brand, cost FROM ranked WHERE rn <= 3;
