WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        CUME_DIST() OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC NULLS LAST) unit_price_cume_dist,
        ROW_NUMBER() OVER (ORDER BY unit_price, id) row_seq,
        LEAD(quantity, 1) OVER w_part next_quantity
    FROM order_items
    WHERE product_id IN (16, 23, 27, 39)
    WINDOW w_part AS (PARTITION BY order_id ORDER BY unit_price, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        unit_price,
        unit_price_cume_dist,
        next_quantity,
        LEAD(next_quantity) OVER w_ordered next_next_quantity,
        LAG(unit_price_cume_dist) OVER w_ordered prev_unit_price_cume_dist,
        ROW_NUMBER() OVER w_ordered seq_no,
        NTILE(4) OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id) unit_price_bucket
    FROM scored
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY row_seq, id)
),
scored_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered seq_no_2,
        NTILE(10) OVER w_hist unit_price_bucket_bucket
    FROM with_windows
    WINDOW w_ordered AS (ORDER BY prev_unit_price_cume_dist, id), w_hist AS (ORDER BY unit_price_bucket DESC, id)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        unit_price,
        unit_price_cume_dist,
        next_next_quantity,
        prev_unit_price_cume_dist,
        seq_no,
        seq_no_2,
        unit_price_bucket_bucket,
        FLOOR(next_quantity) next_quantity_whole
    FROM scored_order_items
    WHERE next_next_quantity = 6
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        unit_price,
        unit_price_cume_dist,
        prev_unit_price_cume_dist,
        seq_no_2,
        unit_price_bucket_bucket,
        next_quantity_whole,
        AVG(unit_price) OVER (ORDER BY next_quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_unit_price,
        SUM(unit_price) OVER (ORDER BY next_quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_unit_price,
        COUNT(unit_price) OVER (ORDER BY next_quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_unit_price,
        MAX(unit_price) OVER (ORDER BY next_quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_unit_price
    FROM base
),
enriched_order_items AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY running_max_unit_price DESC NULLS LAST) running_max_unit_price_cume_dist,
        LEAD(seq_no_2, 1) OVER (ORDER BY next_quantity_whole, id) next_seq_no_2,
        ROW_NUMBER() OVER (ORDER BY running_sum_unit_price DESC, id) event_seq,
        LAG(unit_price, 2, 0) OVER (ORDER BY seq_no_2, id) prev_unit_price,
        NTILE(3) OVER (ORDER BY prev_unit_price_cume_dist DESC, id) prev_unit_price_cume_dist_bucket
    FROM enriched
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, unit_price - unit_price_cume_dist unit_price_minus_unit_price_cume_dist
    FROM enriched_order_items
    WHERE running_max_unit_price_cume_dist <= 0.39
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_max_unit_price_cume_dist >= 0.78 THEN 'HIGH' ELSE 'LOW' END AS running_max_unit_price_cume_dist_tier,
    PERCENTILE_APPROX(running_max_unit_price_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS running_max_unit_price_cume_dist_quartiles,
    SUM(unit_price_cume_dist) AS total_unit_price_cume_dist,
    COUNT(*) AS cnt
FROM cleaned
GROUP BY CASE WHEN running_max_unit_price_cume_dist >= 0.78 THEN 'HIGH' ELSE 'LOW' END
ORDER BY running_max_unit_price_cume_dist_tier DESC NULLS LAST
