SELECT
    id,
    category,
    cost,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) AS rn
FROM products
