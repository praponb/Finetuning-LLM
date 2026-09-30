WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.sale_date < s.launch_date
        )
)
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    stock_qty,
    launch_date,
    tags,
    supplier_id
FROM eligible
ORDER BY stock_qty DESC NULLS LAST, launch_date ASC;
