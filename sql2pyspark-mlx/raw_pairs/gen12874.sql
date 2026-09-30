SELECT
    brand,
    category,
    MAX(launch_date) AS last_launch_date,
    COUNT(CASE WHEN category = 'Electronics' THEN 1 END) AS electronics_count
FROM products
WHERE ARRAY_CONTAINS(tags, 'sale')
GROUP BY brand, category
HAVING COUNT(CASE WHEN category = 'Electronics' THEN 1 END) > 1
