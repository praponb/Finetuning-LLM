WITH ranked AS (SELECT id, brand, price, DENSE_RANK() OVER (PARTITION BY brand ORDER BY price) AS rn FROM products WHERE ARRAY_CONTAINS(tags, 'bestseller')) SELECT * FROM ranked WHERE rn <= 2;
