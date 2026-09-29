WITH cleaned AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        SUBSTRING(product_name, 1, 3) AS product_name_prefix,
        COALESCE(brand, 'UNKNOWN') AS brand_clean
    FROM products
    WHERE launch_date > DATE '2023-09-16'
)
SELECT *
FROM (
    SELECT
        id,
        category,
        price,
        cost
    FROM cleaned
) src
UNPIVOT (
    value FOR metric_name IN (price, cost)
) unp
