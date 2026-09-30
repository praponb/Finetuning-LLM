WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price * 1.5, 2) AS unit_price_adj
    FROM stock_moves
    WHERE move_type IN ('TRANSFER', 'OUTBOUND')
      AND qty > 208
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_adj,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code
    FROM prepared s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.wh_code IS NOT NULL
),
sku_id_stats AS (
    -- Avg unit_price_adj per sku_id
    SELECT sku_id, AVG(unit_price_adj) AS avg_unit_price_adj
    FROM joined
    GROUP BY sku_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_unit_price_adj,
        ROUND(s.unit_price_adj - g.avg_unit_price_adj, 2) AS unit_price_adj_minus_avg
    FROM joined s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.unit_price_adj > g.avg_unit_price_adj
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        MIN(qty) AS min_move_qty,
        ROUND(AVG(unit_price), 2) AS avg_move_unit_price,
        COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.min_move_qty,
        st.avg_move_unit_price,
        COALESCE(st.move_count, 0) AS move_count
    FROM above_average s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
with_parent AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.warehouse_capacity,
        s.warehouse_wh_code,
        s.avg_unit_price_adj,
        s.avg_move_unit_price,
        s.move_count,
        sk.is_active AS sku_is_active,
        sk.labels AS sku_labels,
        sk.sku_code
    FROM with_counts s
    JOIN skus sk ON sk.id = s.sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.operator NOT IN ('op_eve', 'op_dew')) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_operator_count
    FROM with_parent s
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.unit_price,
        s.move_count,
        s.sku_is_active,
        s.sku_code,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.qty AS move_qty,
        st.move_type AS move_move_type
    FROM with_stats s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
)
-- Final projection
SELECT
    id,
    warehouse_id,
    operator,
    unit_price,
    move_id,
    move_qty,
    move_move_type,
    LOWER(REGEXP_REPLACE(TRIM(move_move_type), ' ', '_')) AS move_move_type_slug
FROM with_children_moves
ORDER BY move_id
