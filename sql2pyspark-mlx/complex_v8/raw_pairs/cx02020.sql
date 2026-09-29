WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 9 AND 15
      AND unit_price <= 363.6
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(qty - 199) AS qty_distance,
        CONCAT_WS(': ', move_type_slug, move_type) AS move_type_slug_move_type_label
    FROM valid_moves
    WHERE sku_id IS NOT NULL
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.move_type IN ('TRANSFER', 'INBOUND', 'ADJUST')
              AND st.operator <> 'op_ann'
        )
),
scored AS (
    -- Label and score rows
    SELECT *, ROUND(qty / NULLIF(qty_distance, 0), 3) AS qty_per_qty_distance
    FROM with_activity
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        move_type_slug,
        qty_distance,
        move_type_slug_move_type_label,
        qty_per_qty_distance,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        CASE WHEN qty >= 275 THEN 'A' WHEN qty >= 191 THEN 'B' ELSE 'C' END AS qty_tier,
        CASE WHEN move_type IN ('ADJUST', 'TRANSFER') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
    FROM scored
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) FILTER (WHERE operator = 'op_ann') AS move_operator_count,
        MIN(moved_at) AS first_moved_at
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_operator_count, 0) AS move_operator_count,
        st.first_moved_at
    FROM tagged s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.move_type_slug,
        s.move_type_slug_move_type_label,
        s.move_type_or_unknown,
        s.move_operator_count,
        s.first_moved_at,
        st.id AS move_id,
        st.operator AS move_operator,
        st.move_type AS move_move_type,
        st.moved_at AS move_moved_at,
        st.qty AS move_qty
    FROM with_stats s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
)
-- Final projection
SELECT
    id,
    qty,
    move_type_slug,
    move_type_slug_move_type_label,
    move_type_or_unknown,
    first_moved_at,
    move_id,
    move_operator,
    move_move_type,
    move_moved_at,
    move_qty,
    MONTH(first_moved_at) AS first_moved_at_month,
    LEAST(move_qty, qty) AS least_move_qty_qty
FROM with_children_moves
WHERE sku_id <= 13
ORDER BY first_moved_at_month DESC, move_id DESC NULLS FIRST
