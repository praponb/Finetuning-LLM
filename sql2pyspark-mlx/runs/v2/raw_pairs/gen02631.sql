WITH TopProducts AS (
    SELECT
        id,
        brand,
        stock_qty,
        category,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty, id) AS pos
    FROM products
)
SELECT *
FROM TopProducts
WHERE pos <= 3
