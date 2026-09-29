WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        CASE WHEN stock_qty >= 393 THEN 'High' WHEN stock_qty >= 62 THEN 'Medium' ELSE 'Low' END AS stock_qty_band
    FROM products
    WHERE price < 1020
      AND supplier_id IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY price, id) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY brand ORDER BY price DESC, id) AS price_bucket,
        SUM(price) OVER (PARTITION BY category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_price
    FROM deduped
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ARRAY_CONTAINS(tags, 'clearance') AS is_clearance,
        FLOOR(price) AS price_whole
    FROM with_windows
    WHERE launch_date > DATE '2023-05-27'
      AND supplier_id IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY price DESC, id) AS dedup_rank
    FROM valid
    QUALIFY dedup_rank = 1
),
valid_products AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(running_price, 0) AS running_price_filled,
        DATE_TRUNC('WEEK', launch_date) AS launch_date_period
    FROM latest
    WHERE launch_date > DATE '2023-04-20'
)
SELECT
    id,
    product_name,
    brand,
    price,
    cost,
    launch_date,
    tags,
    stock_qty_band,
    price_bucket,
    CASE WHEN product_name RLIKE '^(Office Chair|Noise-Cancel Headphones)$' THEN 'PRIORITY' WHEN product_name RLIKE '^(Laptop Pro 14|wireless mouse)$' THEN 'CORE' ELSE 'EXTENDED' END AS product_name_zone
FROM valid_products
ORDER BY brand
