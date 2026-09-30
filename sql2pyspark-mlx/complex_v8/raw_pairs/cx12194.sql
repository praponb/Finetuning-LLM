WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        LEAST(qty, unit_price) AS least_qty_unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label
    FROM stock_moves
    WHERE qty > 99
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.unit_price,
        s.least_qty_unit_price,
        s.moved_at_label,
        st.id AS move_id,
        st.qty AS move_qty,
        st.move_type,
        st.operator AS move_operator
    FROM prepared_moves s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.unit_price,
        s.moved_at_label,
        s.move_id,
        s.move_qty,
        s.move_type,
        s.move_operator,
        st.id AS move_id_2,
        st.qty AS move_qty_2,
        st.moved_at AS move_moved_at
    FROM exploded_detail s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM detail_rows s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        unit_price,
        moved_at_label,
        move_id,
        move_qty,
        move_type,
        move_operator,
        move_id_2,
        move_qty_2,
        move_moved_at,
        move_count,
        DATEDIFF(DATE '2026-01-01', move_moved_at) AS days_since_move_moved_at
    FROM summarized_rows
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    unit_price,
    moved_at_label,
    move_id,
    move_qty,
    move_id_2,
    move_moved_at,
    move_count,
    LOWER(REGEXP_REPLACE(TRIM(moved_at_label), ' ', '_')) AS moved_at_label_slug,
    CASE WHEN days_since_move_moved_at >= 595 THEN 'A' WHEN days_since_move_moved_at >= 210 THEN 'B' ELSE 'C' END AS days_since_move_moved_at_tier
FROM tagged
WHERE warehouse_id IS NOT NULL
ORDER BY qty NULLS LAST
