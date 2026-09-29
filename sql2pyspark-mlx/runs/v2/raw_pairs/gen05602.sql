SELECT
    id,
    category,
    launch_date,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date DESC, id) AS row_num
FROM products
WHERE YEAR(launch_date) >= 2023
ORDER BY row_num DESC
