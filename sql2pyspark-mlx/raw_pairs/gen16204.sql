WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LEAST(stock_qty, price) AS least_stock_qty_price
    FROM products
    WHERE brand IS NOT NULL
)
-- Attach suppliers attributes
SELECT
    a.id,
    a.category,
    a.cost,
    a.launch_date,
    a.tags,
    a.supplier_id,
    p.city
FROM cleaned a
LEFT JOIN suppliers p
    ON a.supplier_id = p.id;
