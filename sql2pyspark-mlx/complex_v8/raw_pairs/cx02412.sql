WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT *
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        sk.category AS sku_category,
        sk.is_active AS sku_is_active,
        sk.unit_cost AS sku_unit_cost
    FROM prepared_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        ROUND(AVG(qty), 2) AS avg_move_qty,
        MAX(moved_at) AS last_moved_at,
        MAX(moved_at) AS last_moved_at_2
    FROM stock_moves
    GROUP BY warehouse_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.avg_move_qty,
        st.last_moved_at,
        st.last_moved_at_2
    FROM joined s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.sku_is_active,
        s.sku_unit_cost,
        s.avg_move_qty,
        s.last_moved_at,
        s.last_moved_at_2,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.qty AS move_qty
    FROM profiled s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
)
-- Subtotals with ROLLUP
SELECT
    warehouse_id,
    sku_id,
    GROUPING(warehouse_id) AS is_total_warehouse_id,
    PERCENTILE_APPROX(avg_move_qty, 0.5) AS median_avg_move_qty
FROM line_level
GROUP BY ROLLUP(warehouse_id, sku_id)
ORDER BY warehouse_id NULLS LAST, sku_id NULLS LAST
