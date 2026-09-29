WITH top_products AS (
    SELECT
        id,
        supplier_id,
        launch_date,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date DESC, id) AS pos
    FROM products
)
SELECT id, supplier_id, launch_date
FROM top_products
WHERE pos <= 2
ORDER BY id DESC
