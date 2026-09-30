WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(unit_price * 1.15, 2) AS unit_price_scaled
    FROM order_items
    WHERE order_id = 16
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(unit_price) OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_unit_price,
        AVG(unit_price) OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_unit_price,
        COUNT(unit_price) OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_unit_price
    FROM valid
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM enriched
    INTERSECT
    SELECT id
    FROM order_items
    WHERE unit_price BETWEEN 140 AND 200
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.id = k.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        d.discount,
        d.channel,
        d.status
    FROM kept_rows src
    LEFT JOIN orders d
        ON src.order_id = d.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        quantity,
        channel,
        CUME_DIST() OVER (PARTITION BY status ORDER BY unit_price DESC NULLS FIRST) AS unit_price_cume_dist,
        SUM(unit_price) OVER (PARTITION BY order_id) AS order_id_sum_unit_price,
        DENSE_RANK() OVER (ORDER BY unit_price NULLS LAST) AS unit_price_dense_rank
    FROM with_dims
    WHERE discount > 0.27
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN unit_price_cume_dist >= 0.66 THEN 'PREMIUM' ELSE 'STANDARD' END AS unit_price_cume_dist_tier,
    COUNT(*) FILTER (WHERE channel = 'store') AS store_rows,
    COUNT(DISTINCT channel) AS distinct_channels,
    STDDEV_SAMP(quantity) AS quantity_stddev
FROM scored
GROUP BY unit_price_cume_dist_tier
ORDER BY unit_price_cume_dist_tier NULLS LAST
