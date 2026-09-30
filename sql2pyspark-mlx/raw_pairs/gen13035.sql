SELECT
    id,
    category,
    launch_date,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date, id) AS rn
FROM products
WHERE price > 710
ORDER BY rn DESC
