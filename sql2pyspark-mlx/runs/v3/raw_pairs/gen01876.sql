WITH ranked AS (
    SELECT
        id,
        brand,
        launch_date,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date DESC, id) AS rn
    FROM products
)
SELECT *
FROM ranked
WHERE rn <= 3;
