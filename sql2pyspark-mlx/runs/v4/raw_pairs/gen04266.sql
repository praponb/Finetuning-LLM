WITH valid AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        launch_date,
        tags,
        supplier_id,
        FLOOR(price) AS price_whole,
        stock_qty * price AS stock_qty_times_price,
        COALESCE(brand, 'OTHER') AS brand_clean
    FROM products
    WHERE supplier_id IS NOT NULL
      AND cost < 550
)
SELECT *
FROM (
    SELECT
        category,
        supplier_id,
        brand,
        price_whole
    FROM valid
) src
PIVOT (
    MAX(price_whole) AS value_sum
    FOR brand IN ('Acme' AS acme, 'Umbrella' AS umbrella)
)
