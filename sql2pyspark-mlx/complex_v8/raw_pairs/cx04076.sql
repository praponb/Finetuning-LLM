WITH cleaned_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 7 AND 12
      AND move_type IS NOT NULL
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM cleaned_moves s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
move_stats_2 AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        SUM(unit_price) AS total_move_unit_price,
        COUNT(*) AS move_count_2
    FROM stock_moves
    GROUP BY sku_id
),
with_counts_2 AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.total_move_unit_price,
        COALESCE(st.move_count_2, 0) AS move_count_2
    FROM with_counts s
    LEFT JOIN move_stats_2 st ON st.sku_id = s.sku_id
)
-- Subtotals with GROUPING SETS
SELECT
    sku_id,
    move_count,
    ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
    ROUND(SUM(move_count_2) / NULLIF(SUM(qty), 0), 3) AS move_count_2_to_qty_ratio,
    COUNT(*) AS row_count
FROM with_counts_2
GROUP BY GROUPING SETS ((sku_id, move_count), (sku_id), ())
ORDER BY sku_id NULLS LAST, move_count NULLS LAST
