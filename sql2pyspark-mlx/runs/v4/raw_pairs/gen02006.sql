WITH with_dims AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        s.supplier_name,
        s.country
    FROM products src
    LEFT JOIN suppliers s
        ON src.supplier_id = s.id
)
SELECT
    id,
    product_name,
    category,
    price,
    cost,
    stock_qty,
    launch_date,
    supplier_id,
    supplier_name
FROM with_dims
