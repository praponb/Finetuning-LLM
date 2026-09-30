SELECT
    brand,
    MIN(cost) AS min_cost,
    MIN(launch_date) AS min_launch_date,
    MAX(cost) AS max_cost
FROM products
WHERE ARRAY_CONTAINS(tags, 'premium')
GROUP BY brand
HAVING MIN(cost) >= 560
