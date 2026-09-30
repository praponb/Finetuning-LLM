WITH cleaned_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE YEAR(opened_on) = 2025
      AND LOWER(wh_code) LIKE '%bk%'
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.city,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.operator AS move_operator,
        st.unit_price AS move_unit_price,
        st.sku_id AS move_sku_id
    FROM cleaned_warehouses s
    JOIN stock_moves st ON st.warehouse_id = s.id
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        SUM(unit_price) AS total_move_unit_price,
        MAX(moved_at) AS last_moved_at
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.total_move_unit_price,
        st.last_moved_at
    FROM with_children_moves s
    LEFT JOIN move_stats st ON st.warehouse_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    move_id,
    move_sku_id,
    ROUND(100 * COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) / COUNT(*), 1) AS pct_city_bangkok
FROM with_stats
GROUP BY GROUPING SETS ((move_id, move_sku_id), (move_id), (move_sku_id), ())
ORDER BY move_id NULLS LAST, move_sku_id NULLS LAST
