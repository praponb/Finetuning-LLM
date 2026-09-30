WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        unit_price,
        AVG(quantity) OVER (ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) quantity_avg_6,
        FIRST_VALUE(unit_price, TRUE) OVER (PARTITION BY order_id, product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_unit_price
    FROM order_items
    WHERE order_id IN (29, 35)
),
enriched_order_items AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(quantity_avg_6) OVER w_ordered last7_count_quantity_avg_6,
        SUM(quantity_avg_6) OVER w_ordered last7_sum_quantity_avg_6,
        MIN(quantity_avg_6) OVER w_ordered last7_min_quantity_avg_6
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY first_known_unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
enriched_order_items_2 AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.last7_min_quantity_avg_6,
        d.brand,
        d.product_name,
        d.category
    FROM enriched_order_items s
    LEFT JOIN products d
        ON s.product_id = d.id
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.category AS product_category,
        p.brand AS product_brand
    FROM enriched_order_items_2 src
    JOIN products p
        ON src.product_id = p.id
),
enriched_order_items_3 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        last7_min_quantity_avg_6,
        product_name,
        category,
        product_category,
        SUM(last7_min_quantity_avg_6) OVER w_ordered running_last7_min_quantity_avg_6,
        ROUND(last7_min_quantity_avg_6 / SUM(last7_min_quantity_avg_6) OVER (PARTITION BY product_id), 4) last7_min_quantity_avg_6_share
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY category ORDER BY last7_min_quantity_avg_6, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        last7_min_quantity_avg_6,
        product_name,
        category,
        running_last7_min_quantity_avg_6,
        last7_min_quantity_avg_6_share,
        PERCENT_RANK() OVER (ORDER BY running_last7_min_quantity_avg_6 DESC) running_last7_min_quantity_avg_6_percent_rank,
        LEAD(last7_min_quantity_avg_6_share, 2) OVER (PARTITION BY product_category ORDER BY last7_min_quantity_avg_6, id) next_last7_min_quantity_avg_6_share
    FROM enriched_order_items_3
),
joined AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.category,
        e.running_last7_min_quantity_avg_6,
        e.last7_min_quantity_avg_6_share,
        e.next_last7_min_quantity_avg_6_share,
        b.price,
        b.cost
    FROM scored e
    LEFT JOIN products b
        ON e.product_id <=> b.id
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        category,
        CASE WHEN last7_min_quantity_avg_6_share >= 0.79 THEN 'PREMIUM' ELSE 'STANDARD' END AS last7_min_quantity_avg_6_share_tier,
        SIZE(COLLECT_SET(category)) AS num_categories,
        COUNT(*) AS row_count,
        COUNT(CASE WHEN category = 'Home' THEN 1 END) AS home_count,
        COUNT_IF(category = 'Home') AS home_events,
        APPROX_PERCENTILE(running_last7_min_quantity_avg_6, 0.9) AS running_last7_min_quantity_avg_6_p90,
        PERCENT_RANK() OVER (PARTITION BY category ORDER BY APPROX_PERCENTILE(running_last7_min_quantity_avg_6, 0.9) DESC) AS running_last7_min_quantity_avg_6_p90_percent_rank
    FROM joined
    GROUP BY category, CASE WHEN last7_min_quantity_avg_6_share >= 0.79 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    category,
    last7_min_quantity_avg_6_share_tier,
    num_categories,
    row_count,
    home_count,
    running_last7_min_quantity_avg_6_p90,
    running_last7_min_quantity_avg_6_p90_percent_rank
FROM daily
ORDER BY home_count DESC NULLS LAST
