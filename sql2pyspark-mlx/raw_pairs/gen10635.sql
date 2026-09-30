WITH joined AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.units,
        s.revenue,
        d.stock_qty,
        d.category
    FROM store_sales s
    INNER JOIN products d
        ON s.product_id = d.id
    WHERE d.category IN ('Sports', 'Books')
)
SELECT
    id,
    units,
    revenue,
    stock_qty
FROM joined;
