WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        LENGTH(TRIM(operator)) AS operator_len,
        qty - unit_price AS qty_minus_unit_price
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM scoped_moves s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.move_type IN ('ADJUST', 'OUTBOUND', 'TRANSFER')
              AND st.qty <= 415
        )
      AND s.qty_minus_unit_price >= 300.7
),
with_parent_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator_len,
        sk.is_active AS sku_is_active
    FROM screened s
    JOIN skus sk ON sk.id = s.sku_id
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_parent_sku s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
        )
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    sku_is_active,
    DATE_FORMAT(moved_at, 'yyyy-MM-dd') AS moved_at_label,
    CASE WHEN move_type IN ('TRANSFER', 'OUTBOUND') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group
FROM without_matches
WHERE NOT sku_is_active
