WITH enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.order_date,
        o.discount,
        o.channel
    FROM order_items src
    LEFT JOIN orders o
        ON src.order_id = o.id
    WHERE o.customer_id IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT order_id, product_id
    FROM enriched
    EXCEPT
    SELECT order_id, product_id
    FROM order_items
    WHERE product_id = 18
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.product_id = k.product_id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount,
        FLOOR(unit_price) AS unit_price_whole,
        COALESCE(channel, 'N/A') AS channel_clean
    FROM reconciled
    WHERE product_id IS NOT NULL
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.order_id,
        src.product_id,
        src.unit_price,
        src.unit_price_whole,
        src.channel_clean,
        b.product_name
    FROM cleaned src
    INNER JOIN products b
        ON src.product_id <=> b.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN unit_price_whole >= 72 THEN 'HIGH' ELSE 'LOW' END AS unit_price_whole_tier,
    order_id,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    COUNT(*) AS row_count,
    COUNT(*) AS row_count_2,
    PERCENTILE_APPROX(unit_price_whole, 0.9) AS unit_price_whole_p90
FROM with_dims
GROUP BY unit_price_whole_tier, order_id
