WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 7 AND 10
      AND qty < 299
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, MIN(moved_at) AS first_moved_at
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.first_moved_at
    FROM cleaned_moves s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
sku_id_stats AS (
    -- Median qty per sku_id
    SELECT sku_id, PERCENTILE_APPROX(qty, 0.5) AS median_qty
    FROM summarized_rows
    GROUP BY sku_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_qty, ROUND(s.qty - g.median_qty, 2) AS qty_minus_median
    FROM summarized_rows s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.qty >= g.median_qty
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM outliers
    WHERE median_qty > (SELECT AVG(median_qty) FROM outliers)
)
-- Subtotals with GROUPING SETS
SELECT
    warehouse_id,
    sku_id,
    move_type,
    ROUND(SUM(median_qty) / NULLIF(SUM(qty), 0), 3) AS median_qty_to_qty_ratio,
    ROUND(SUM(median_qty) / NULLIF(SUM(qty_minus_median), 0), 3) AS median_qty_to_qty_minus_median_ratio,
    ROUND(SUM(median_qty), 2) AS total_median_qty
FROM screened
GROUP BY GROUPING SETS ((warehouse_id, sku_id, move_type), (warehouse_id), (sku_id), ())
ORDER BY warehouse_id NULLS LAST, sku_id NULLS LAST, move_type NULLS LAST
