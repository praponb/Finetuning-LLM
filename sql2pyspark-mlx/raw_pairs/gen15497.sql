WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC, id) AS row_num
    FROM products
    QUALIFY row_num = 1
)
SELECT
    id,
    product_name,
    brand,
    price,
    cost,
    stock_qty,
    supplier_id,
    row_num
FROM latest
ORDER BY price DESC NULLS FIRST
