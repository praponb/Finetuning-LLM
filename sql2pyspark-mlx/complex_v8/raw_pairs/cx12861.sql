WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        CONCAT_WS('-', operator, move_type) AS operator_move_type_label,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix
    FROM stock_moves
    WHERE (warehouse_id IS NOT NULL OR sku_id >= 8)
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        MAX(moved_at) AS last_moved_at,
        MIN(moved_at) AS first_moved_at,
        COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at,
        st.first_moved_at,
        COALESCE(st.move_count, 0) AS move_count
    FROM scoped s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.operator_move_type_label,
        s.move_type_prefix,
        s.last_moved_at,
        s.first_moved_at,
        s.move_count,
        sk.sku_code,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code
    FROM with_stats s
    JOIN skus sk ON sk.id = s.sku_id AND sk.weight_kg <= 14.6
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.qty BETWEEN 102 AND 422
              AND st.sku_id <= 10
        )
),
with_parent AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.move_type_prefix,
        s.last_moved_at,
        s.first_moved_at,
        s.move_count,
        s.sku_code,
        s.warehouse_wh_code,
        sk.sku_code AS sku_sku_code,
        w.capacity AS warehouse_capacity
    FROM filtered s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    operator,
    move_type_prefix,
    last_moved_at,
    move_count,
    warehouse_wh_code,
    sku_sku_code,
    SUBSTRING(TRIM(move_type_prefix), 1, 2) AS move_type_prefix_prefix,
    CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label
FROM with_parent
ORDER BY move_type NULLS LAST, id
LIMIT 20
