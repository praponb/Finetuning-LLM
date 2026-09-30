WITH filtered_products AS (
    SELECT *
    FROM products
    WHERE stock_qty < 153
),
summary AS (
    SELECT brand, MIN(stock_qty) AS min_stock_qty
    FROM filtered_products
    GROUP BY brand
)
SELECT *
FROM summary
WHERE min_stock_qty < 320;
