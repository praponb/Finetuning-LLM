SELECT
    id,
    supplier_id,
    launch_date,
    product_name
FROM (
    SELECT
        id,
        supplier_id,
        launch_date,
        product_name,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date DESC, id) AS row_num
    FROM products
    WHERE launch_date > '2024-05-01'
) AS t
WHERE row_num <= 5
ORDER BY supplier_id
