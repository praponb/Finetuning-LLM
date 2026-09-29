WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix,
        CASE WHEN unit_price >= 290.4 THEN 'A' WHEN unit_price >= 238.5 THEN 'B' ELSE 'C' END AS unit_price_tier
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
      AND unit_price > 124.1
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        sku_id,
        warehouse_id,
        MAX(moved_at) AS last_moved_at,
        COUNT(*) AS cnt,
        ROUND(STDDEV(qty), 2) AS stddev_qty,
        PERCENTILE_APPROX(qty, 0.5) AS median_qty,
        ROUND(AVG(qty), 2) AS avg_qty
    FROM filtered_moves
    GROUP BY sku_id, warehouse_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(stddev_qty) OVER (ORDER BY avg_qty, sku_id, warehouse_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_stddev_qty,
        SUM(median_qty) OVER (ORDER BY avg_qty, sku_id, warehouse_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS median_qty_sum_2_prior
    FROM per_group
)
SELECT
    warehouse_id,
    cnt,
    GROUPING_ID() AS grouping_level,
    MIN(last_moved_at) AS first_last_moved_at,
    ROUND(AVG(avg_qty), 2) AS avg_avg_qty,
    ROUND(STDDEV(stddev_qty), 2) AS stddev_stddev_qty
FROM metrics
GROUP BY CUBE(warehouse_id, cnt)
