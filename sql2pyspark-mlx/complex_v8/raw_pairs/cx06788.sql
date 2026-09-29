WITH valid AS (
    -- Filter stock_moves
    SELECT *
    FROM stock_moves
    WHERE operator <> 'op_dew'
      AND unit_price <= 393.3
),
enriched AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.unit_price,
        sk.is_active AS sku_is_active,
        sk.unit_cost AS sku_unit_cost
    FROM valid s
    JOIN skus sk ON sk.id = s.sku_id
),
move_type_stats AS (
    -- Median qty per move_type
    SELECT move_type, PERCENTILE_APPROX(qty, 0.5) AS median_qty
    FROM enriched
    GROUP BY move_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_qty, ROUND(s.qty - g.median_qty, 2) AS qty_minus_median
    FROM enriched s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.qty >= g.median_qty
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.sku_is_active,
        s.median_qty,
        st.id AS move_id,
        st.qty AS move_qty,
        st.move_type AS move_move_type
    FROM vs_group s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at
    FROM with_children s
)
-- Final projection
SELECT
    id,
    move_type,
    moved_at,
    sku_is_active,
    median_qty,
    move_id,
    move_qty,
    move_move_type,
    last_moved_at
FROM with_counts
ORDER BY id DESC, move_qty DESC
