SELECT
    id,
    category,
    cost,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) AS rn
FROM products
WHERE cost > 210
ORDER BY category ASC, cost ASC;
