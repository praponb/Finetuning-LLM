WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        last_restock,
        CASE WHEN quantity >= 249 THEN 'High' WHEN quantity >= 77 THEN 'Medium' ELSE 'Low' END AS quantity_tier,
        COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE last_restock >= DATE '2023-02-12'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
latest_inventory AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        product_id,
        quantity_tier
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY latest_rank) = 1
),
joined AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        p.price,
        p.brand
    FROM latest_inventory src
    LEFT JOIN products p
        ON src.product_id = p.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        price,
        SUM(price) OVER w_recent AS cumulative_price,
        FIRST_VALUE(brand, TRUE) OVER (PARTITION BY product_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_brand,
        LAST_VALUE(price, TRUE) OVER w_recent AS last_known_price
    FROM joined
    WINDOW w_recent AS (PARTITION BY product_id, brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    CASE WHEN price >= 1220 THEN 'HIGH' ELSE 'LOW' END AS price_tier,
    PERCENTILE_APPROX(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
    COUNT(*) AS num_rows,
    MIN(price) AS min_price,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows_2,
    SUM(cumulative_price) AS total_cumulative_price
FROM with_windows
GROUP BY CASE WHEN price >= 1220 THEN 'HIGH' ELSE 'LOW' END
HAVING num_rows_2 >= 1;
