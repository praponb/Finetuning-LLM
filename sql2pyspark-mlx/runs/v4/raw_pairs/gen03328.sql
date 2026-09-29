SELECT
    id,
    category,
    price,
    NTILE(3) OVER (PARTITION BY category ORDER BY price DESC, id) AS quartile
FROM products
WHERE ARRAY_CONTAINS(tags, 'clearance')
