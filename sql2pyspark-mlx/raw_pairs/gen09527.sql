WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        LEAST(unit_price, quantity) AS least_unit_price_quantity,
        ROUND(unit_price, 0) AS unit_price_rounded
    FROM order_items
    WHERE product_id IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        unit_price_rounded
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY least_unit_price_quantity DESC NULLS LAST, id) AS rank_in_group
        FROM valid
    ) t
    WHERE rank_in_group = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(unit_price_rounded) OVER w_hist AS last3_min_unit_price_rounded,
        SUM(unit_price_rounded) OVER w_hist AS last3_sum_unit_price_rounded,
        AVG(unit_price_rounded) OVER w_hist AS last3_avg_unit_price_rounded,
        AVG(quantity) OVER (PARTITION BY product_id) AS product_id_avg_quantity
    FROM latest
    WHERE unit_price_rounded IS NOT NULL
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY unit_price_rounded, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last3_avg_unit_price_rounded, id) = 1
),
deduped_order_items AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        unit_price_rounded,
        last3_avg_unit_price_rounded,
        product_id_avg_quantity
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price_rounded NULLS FIRST, id) AS row_num
        FROM deduped
    ) t
    WHERE row_num = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(unit_price_rounded) OVER (ORDER BY last3_avg_unit_price_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price_rounded,
        NTILE(10) OVER (ORDER BY product_id_avg_quantity, id) AS product_id_avg_quantity_bucket
    FROM deduped_order_items
),
valid_order_items AS (
    -- Keep recent, valid rows
    SELECT
        id,
        last3_avg_unit_price_rounded,
        product_id_avg_quantity,
        running_unit_price_rounded,
        CASE WHEN product_id_avg_quantity >= 8 THEN 'High' WHEN product_id_avg_quantity >= 3 THEN 'Medium' ELSE 'Low' END AS product_id_avg_quantity_level
    FROM with_windows
    WHERE last3_avg_unit_price_rounded < 63
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_unit_price_rounded >= 402 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_unit_price_rounded_tier,
    COUNT(*) AS row_count,
    COUNT(*) AS cnt,
    SUM(product_id_avg_quantity) AS total_product_id_avg_quantity,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(running_unit_price_rounded, ARRAY(0.25, 0.5, 0.75)) AS running_unit_price_rounded_quartiles,
    GROUPING_ID() AS grouping_id
FROM valid_order_items
GROUP BY ROLLUP (CASE WHEN running_unit_price_rounded >= 402 THEN 'PREMIUM' ELSE 'STANDARD' END);
