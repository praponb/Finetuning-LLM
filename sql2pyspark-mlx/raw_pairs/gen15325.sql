WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        ROUND(unit_price * 0.9, 2) AS unit_price_adjusted
    FROM order_items
    WHERE order_id = 1
),
base_order_items AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN unit_price >= 390 THEN 'High' WHEN unit_price >= 140 THEN 'Medium' ELSE 'Low' END AS unit_price_band
    FROM base
    WHERE product_id IS NOT NULL
      AND product_id IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        quantity,
        unit_price,
        unit_price_band,
        unit_price * quantity AS line_total
    FROM base_order_items
    WHERE product_id IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        unit_price,
        line_total,
        LAST_VALUE(unit_price) IGNORE NULLS OVER w_hist AS last_known_unit_price,
        DENSE_RANK() OVER w_grp AS unit_price_dense_rank,
        LAST_VALUE(quantity) IGNORE NULLS OVER w_running AS last_known_quantity,
        AVG(line_total) OVER w_ordered AS line_total_avg_5,
        SUM(quantity) OVER w_running AS cumulative_quantity
    FROM filtered
    WHERE line_total < 73
    WINDOW w_hist AS (ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY unit_price DESC NULLS FIRST), w_running AS (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (ORDER BY unit_price, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(line_total_avg_5) OVER w_ordered AS running_line_total_avg_5,
        LAST_VALUE(last_known_quantity, TRUE) OVER w_part AS last_known_last_known_quantity,
        FIRST_VALUE(unit_price_dense_rank) IGNORE NULLS OVER (ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_dense_rank,
        PERCENT_RANK() OVER w_running AS unit_price_dense_rank_percent_rank
    FROM enriched
    WINDOW w_ordered AS (ORDER BY last_known_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (ORDER BY unit_price_dense_rank DESC NULLS FIRST)
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN first_known_unit_price_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS first_known_unit_price_dense_rank_tier,
        CASE WHEN last_known_last_known_quantity >= 7 THEN 'LARGE' ELSE 'SMALL' END AS last_known_last_known_quantity_tier,
        SUM(line_total) AS total_line_total,
        APPROX_PERCENTILE(line_total, 0.5) AS line_total_p50
    FROM metrics
    GROUP BY GROUPING SETS ((CASE WHEN first_known_unit_price_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN last_known_last_known_quantity >= 7 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN first_known_unit_price_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
)
SELECT total_line_total, line_total_p50
FROM stats
ORDER BY line_total_p50 DESC
