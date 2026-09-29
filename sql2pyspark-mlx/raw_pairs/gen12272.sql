WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        ROUND(quantity, 1) AS quantity_rounded
    FROM order_items
    WHERE product_id IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity_rounded,
        LEAST(quantity_rounded, quantity) AS least_quantity_rounded_quantity
    FROM base
    WHERE order_id = 22
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        quantity_rounded,
        AVG(least_quantity_rounded_quantity) OVER w_hist AS least_quantity_rounded_quantity_avg_2,
        AVG(unit_price) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS unit_price_avg_3
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY product_id, order_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_rounded DESC NULLS FIRST, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        product_id,
        least_quantity_rounded_quantity_avg_2,
        unit_price_avg_3
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_rounded NULLS FIRST, id) AS latest_rank
        FROM deduped
    ) x
    WHERE latest_rank = 1
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.least_quantity_rounded_quantity_avg_2,
        e.unit_price_avg_3,
        b.launch_date
    FROM ranked e
    LEFT JOIN products b
        ON e.product_id = b.id
    WHERE SIZE(b.tags) > 2
),
enriched_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        least_quantity_rounded_quantity_avg_2,
        unit_price_avg_3,
        LAG(least_quantity_rounded_quantity_avg_2) OVER w_grp AS prev_least_quantity_rounded_quantity_avg_2,
        LEAD(least_quantity_rounded_quantity_avg_2) OVER w_grp AS next_least_quantity_rounded_quantity_avg_2,
        LAG(unit_price_avg_3) OVER w_grp AS prev_unit_price_avg_3,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM with_dims
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY launch_date, id)
),
summary AS (
    -- Multi-level aggregation
    SELECT
        product_id,
        CASE WHEN event_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
        MAX(prev_least_quantity_rounded_quantity_avg_2) AS max_prev_least_quantity_rounded_quantity_avg_2,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(prev_unit_price_avg_3, ARRAY(0.25, 0.5, 0.75)) AS prev_unit_price_avg_3_quartiles,
        MAX(least_quantity_rounded_quantity_avg_2) AS max_least_quantity_rounded_quantity_avg_2
    FROM enriched_order_items
    GROUP BY ROLLUP (product_id, CASE WHEN event_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END)
)
SELECT event_seq_tier, max_least_quantity_rounded_quantity_avg_2
FROM summary
ORDER BY event_seq_tier DESC
