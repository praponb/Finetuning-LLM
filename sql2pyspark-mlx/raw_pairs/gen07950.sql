WITH remaining_keys AS (
    -- Set difference on keys
    SELECT order_id, product_id
    FROM order_items
    EXCEPT
    SELECT order_id, product_id
    FROM order_items
    WHERE order_id IN (18, 30)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id AND s.product_id = k.product_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        RANK() OVER (ORDER BY unit_price ASC NULLS LAST) AS unit_price_rank
    FROM kept_rows
),
metrics_order_items AS (
    -- Running and moving aggregates
    SELECT
        product_id,
        quantity,
        unit_price,
        PERCENT_RANK() OVER (ORDER BY unit_price DESC NULLS FIRST) AS unit_price_percent_rank,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY order_id, product_id), 4) AS unit_price_share,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY order_id), 4) AS unit_price_share_2,
        CUME_DIST() OVER (ORDER BY unit_price_rank ASC NULLS FIRST) AS unit_price_rank_cume_dist,
        MIN(unit_price_rank) OVER (PARTITION BY order_id) AS order_id_min_unit_price_rank
    FROM metrics
    WHERE unit_price_rank <= 3
),
scored AS (
    -- Running and moving aggregates
    SELECT
        product_id,
        unit_price,
        unit_price_share,
        unit_price_share_2,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id), 4) AS quantity_share,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY unit_price_rank_cume_dist DESC) AS unit_price_rank_cume_dist_percent_rank,
        CORR(unit_price, quantity) OVER (PARTITION BY product_id) AS corr_unit_price_quantity,
        SUM(unit_price_percent_rank) OVER (PARTITION BY product_id) AS product_id_sum_unit_price_percent_rank
    FROM metrics_order_items
)
-- Multi-level aggregation
SELECT
    CASE WHEN unit_price >= 340 THEN 'PREMIUM' ELSE 'STANDARD' END AS unit_price_tier,
    product_id,
    APPROX_PERCENTILE(unit_price_rank_cume_dist_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS unit_price_rank_cume_dist_percent_rank_quartiles,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    AVG(unit_price) AS avg_unit_price,
    PERCENTILE_APPROX(quantity_share, 0.9) AS quantity_share_p90,
    SUM(unit_price_share) AS total_unit_price_share,
    COUNT(*) AS num_rows
FROM scored
GROUP BY CASE WHEN unit_price >= 340 THEN 'PREMIUM' ELSE 'STANDARD' END, product_id;
