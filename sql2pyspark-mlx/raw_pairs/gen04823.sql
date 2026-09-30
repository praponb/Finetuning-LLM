WITH ranked AS (
    SELECT
        id,
        supplier_id,
        launch_date,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date DESC, id) AS rn
    FROM products
    WHERE price <= 610
)
SELECT id, supplier_id, launch_date
FROM ranked
WHERE rn <= 3
ORDER BY id DESC, launch_date
