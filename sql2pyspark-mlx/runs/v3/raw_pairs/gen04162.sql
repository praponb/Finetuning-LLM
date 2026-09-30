SELECT id, category, launch_date
FROM (
    SELECT
        id,
        category,
        launch_date,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY launch_date) rn
    FROM products
) t
WHERE rn <= 3
ORDER BY category, id DESC;
