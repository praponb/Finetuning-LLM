WITH product_summary AS (
    SELECT product_id, MIN(units) AS min_units, ROUND(STDDEV(units), 2) AS stddev_units
    FROM store_sales
    WHERE product_id IN (3, 7)
    GROUP BY product_id
)
SELECT
    products.id,
    products.brand,
    products.stock_qty,
    product_summary.min_units,
    product_summary.stddev_units
FROM products
INNER JOIN product_summary ON products.id = product_summary.product_id
WHERE product_summary.stddev_units >= 39
ORDER BY stock_qty DESC
