WITH ranked AS (
    SELECT
        id,
        supplier_id,
        cost,
        launch_date,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY cost DESC, id) AS rn
    FROM products
)
SELECT
    id,
    supplier_id,
    cost,
    launch_date
FROM ranked
WHERE rn <= 3;
