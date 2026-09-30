WITH numbered AS (
    SELECT
        id,
        brand,
        cost,
        launch_date,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost DESC, id) AS pos
    FROM products
)
SELECT
    id,
    brand,
    cost,
    launch_date
FROM numbered
WHERE pos <= 1
ORDER BY id DESC
