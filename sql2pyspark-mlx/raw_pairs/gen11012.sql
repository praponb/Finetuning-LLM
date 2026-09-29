WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(quantity, 1) AS quantity_rounded
    FROM inventory
    WHERE last_restock > DATE '2023-09-08'
),
base_inventory AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_TRUNC('month', last_restock) AS last_restock_period,
        CASE WHEN quantity >= 246 THEN 'High' WHEN quantity >= 95 THEN 'Medium' ELSE 'Low' END AS quantity_level
    FROM base
    WHERE last_restock >= TO_DATE('2023-05-18')
      AND product_id IN (2, 5, 7, 10)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base_inventory
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY last_restock) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        quantity_rounded,
        last_restock_period,
        quantity_level,
        LEAD(quantity_rounded, 1) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS next_quantity_rounded,
        SUM(quantity_rounded) OVER (PARTITION BY product_id, store_id) AS product_id_sum_quantity_rounded,
        COUNT(quantity) OVER (ORDER BY CAST(last_restock_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS quantity_count_30d,
        SUM(quantity_rounded) OVER (PARTITION BY YEAR(last_restock), store_id) AS last_restock_year_sum_quantity_rounded,
        DENSE_RANK() OVER (PARTITION BY store_id, product_id ORDER BY quantity_rounded NULLS LAST) AS quantity_rounded_dense_rank
    FROM ranked
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        quantity_rounded,
        last_restock_period,
        next_quantity_rounded,
        product_id_sum_quantity_rounded,
        last_restock_year_sum_quantity_rounded,
        AVG(quantity_rounded) OVER (ORDER BY last_restock_year_sum_quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_quantity_rounded,
        MIN(quantity_rounded) OVER (ORDER BY last_restock_year_sum_quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_quantity_rounded,
        COUNT(quantity_rounded) OVER (ORDER BY last_restock_year_sum_quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_quantity_rounded
    FROM with_windows
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(running_min_quantity_rounded) OVER w_hist AS running_running_min_quantity_rounded,
        SUM(last_restock_year_sum_quantity_rounded) OVER (ORDER BY last_restock_year_sum_quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last_restock_year_sum_quantity_rounded
    FROM metrics
    WINDOW w_hist AS (ORDER BY running_min_quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(running_running_min_quantity_rounded) OVER (ORDER BY running_min_quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_running_running_min_quantity_rounded,
        COUNT(running_running_min_quantity_rounded) OVER (ORDER BY running_min_quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_running_min_quantity_rounded,
        SUM(running_running_min_quantity_rounded) OVER (ORDER BY running_min_quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_running_min_quantity_rounded
    FROM enriched
    WHERE running_running_min_quantity_rounded != 251
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', last_restock_period) AS year_start,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(next_quantity_rounded, ARRAY(0.25, 0.5, 0.75)) AS next_quantity_rounded_quartiles,
    MAX(last_restock_period) AS last_seen,
    MIN(quantity_rounded) AS min_quantity_rounded,
    AVG(cumulative_last_restock_year_sum_quantity_rounded) AS avg_cumulative_last_restock_year_sum_quantity_rounded,
    SUM(quantity_rounded) AS total_quantity_rounded,
    GROUPING_ID() AS gid
FROM enriched_inventory
GROUP BY ROLLUP (DATE_TRUNC('YEAR', last_restock_period))
ORDER BY min_quantity_rounded DESC
