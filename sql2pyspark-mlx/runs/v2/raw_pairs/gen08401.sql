WITH base AS (
    SELECT *
    FROM inventory
    WHERE store_id = 4
),
last_restock_year_summary AS (
    SELECT
        YEAR(last_restock) AS last_restock_year,
        product_id,
        ROUND(STDDEV(quantity), 2) AS stddev_quantity,
        MAX(quantity) AS max_quantity
    FROM base
    GROUP BY YEAR(last_restock), product_id
)
SELECT
    last_restock_year,
    product_id,
    stddev_quantity,
    max_quantity
FROM last_restock_year_summary
WHERE max_quantity < 180
