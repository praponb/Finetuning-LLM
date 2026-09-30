WITH ranked AS (
    SELECT
        id,
        product_name,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id
    FROM products
    QUALIFY RANK() OVER (PARTITION BY brand, supplier_id ORDER BY stock_qty DESC NULLS LAST) = 1
)
SELECT
    id,
    product_name,
    price,
    stock_qty
FROM ranked
ORDER BY id
