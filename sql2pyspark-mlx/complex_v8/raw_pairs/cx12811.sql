WITH scoped_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        LOWER(TRIM(category)) AS category_clean
    FROM skus
    WHERE category IS NOT NULL
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        ROUND(AVG(unit_price), 2) AS avg_move_unit_price,
        MAX(moved_at) AS last_moved_at,
        COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.avg_move_unit_price,
        st.last_moved_at,
        COALESCE(st.move_count, 0) AS move_count
    FROM scoped_skus s
    LEFT JOIN move_stats st ON st.sku_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    move_count,
    is_active,
    SUM(avg_move_unit_price) AS total_avg_move_unit_price,
    PERCENTILE_APPROX(move_count, 0.5) AS median_move_count
FROM with_counts
GROUP BY GROUPING SETS ((move_count, is_active), (move_count), (is_active))
