WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        unit_price,
        FLOOR(quantity) AS quantity_whole
    FROM order_items
    WHERE quantity IS NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(quantity_whole * 0.85, 2) AS quantity_whole_adjusted,
        unit_price + quantity_whole AS unit_price_plus_quantity_whole
    FROM base
    WHERE unit_price < 360
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(unit_price) OVER (PARTITION BY order_id ORDER BY quantity_whole, id) AS next_unit_price,
        LAG(quantity_whole_adjusted) OVER (PARTITION BY order_id ORDER BY quantity_whole, id) AS prev_quantity_whole_adjusted,
        LEAD(quantity_whole_adjusted) OVER (PARTITION BY order_id ORDER BY quantity_whole, id) AS next_quantity_whole_adjusted,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity_whole, id) AS event_seq,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY quantity_whole NULLS FIRST) AS quantity_whole_dense_rank
    FROM filtered
    WHERE quantity_whole_adjusted BETWEEN 33 AND 79
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity_whole_adjusted,
        unit_price_plus_quantity_whole,
        next_unit_price,
        prev_quantity_whole_adjusted,
        next_quantity_whole_adjusted,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY event_seq NULLS FIRST) AS event_seq_cume_dist,
        AVG(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS unit_price_avg_2
    FROM enriched
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity_whole_adjusted,
        next_unit_price,
        prev_quantity_whole_adjusted,
        event_seq_cume_dist,
        unit_price_avg_2,
        SUM(next_unit_price) OVER (ORDER BY unit_price_plus_quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_unit_price,
        SUM(next_unit_price) OVER (ORDER BY next_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_unit_price
    FROM with_windows
)
-- Multi-level aggregation
SELECT
    CASE WHEN next_unit_price >= 440 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_unit_price_tier,
    CASE WHEN event_seq_cume_dist >= 0.66 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_cume_dist_tier,
    CASE WHEN quantity_whole_adjusted >= 76 THEN 'HIGH' ELSE 'LOW' END AS quantity_whole_adjusted_tier,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(event_seq_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS event_seq_cume_dist_quartiles,
    COUNT(*) AS event_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM metrics
GROUP BY CASE WHEN next_unit_price >= 440 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN event_seq_cume_dist >= 0.66 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN quantity_whole_adjusted >= 76 THEN 'HIGH' ELSE 'LOW' END
