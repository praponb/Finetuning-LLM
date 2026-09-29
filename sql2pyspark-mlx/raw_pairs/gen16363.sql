WITH enriched AS (
    SELECT
        e.*,
        b.price,
        b.stock_qty,
        b.category
    FROM store_sales e
    LEFT JOIN products b
        ON e.product_id = b.id
)
SELECT
    id,
    store_id,
    product_id,
    units,
    revenue
FROM enriched
ORDER BY product_id ASC;
