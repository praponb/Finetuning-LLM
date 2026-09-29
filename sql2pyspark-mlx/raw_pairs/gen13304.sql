WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        SUM(unit_price) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price,
        CUME_DIST() OVER (ORDER BY unit_price DESC NULLS LAST) AS unit_price_cume_dist,
        NTILE(3) OVER (ORDER BY quantity DESC, id) AS quantity_bucket,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY order_id, product_id), 4) AS quantity_share
    FROM order_items
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        cumulative_unit_price,
        unit_price_cume_dist,
        quantity_share,
        cumulative_unit_price + unit_price AS cumulative_unit_price_plus_unit_price
    FROM scored
    WHERE order_id = 36
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, quantity_share + cumulative_unit_price_plus_unit_price AS quantity_share_plus_cumulative_unit_price_plus_unit_price
    FROM valid
    WHERE quantity_share <= 0.48
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY order_id), 4) AS unit_price_share,
        SUM(quantity_share) OVER (PARTITION BY order_id ORDER BY cumulative_unit_price_plus_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_share,
        RANK() OVER (PARTITION BY order_id ORDER BY quantity_share DESC NULLS LAST) AS quantity_share_rank
    FROM cleaned
),
enriched_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(quantity_share_rank) OVER w_ordered AS prev_quantity_share_rank,
        LEAD(quantity_share_rank) OVER w_ordered AS next_quantity_share_rank,
        LEAD(running_quantity_share) OVER w_ordered AS next_running_quantity_share,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity_share_plus_cumulative_unit_price_plus_unit_price DESC, id) AS seq_no
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY quantity, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(seq_no) OVER w_recent AS running_min_seq_no,
        COUNT(seq_no) OVER w_recent AS running_count_seq_no,
        SUM(seq_no) OVER w_recent AS running_sum_seq_no,
        MAX(seq_no) OVER w_recent AS running_max_seq_no
    FROM enriched_order_items
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY cumulative_unit_price_plus_unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(cumulative_unit_price_plus_unit_price, 0.95) AS cumulative_unit_price_plus_unit_price_p95,
    ROUND(AVG(quantity_share), 2) AS avg_quantity_share
FROM metrics
GROUP BY CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END
