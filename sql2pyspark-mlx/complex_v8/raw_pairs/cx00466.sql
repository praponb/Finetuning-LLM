WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        TO_DATE(moved_at) AS moved_at_date,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        CONCAT_WS('|', move_type, operator) AS move_type_operator_label
    FROM stock_moves
    WHERE (TO_DATE(moved_at) < DATE '2025-03-02' OR operator IS NOT NULL)
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM base_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.move_type = 'INBOUND'
        )
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.move_type IS NOT NULL
        )
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at,
    operator,
    moved_at_date,
    FLOOR(qty / 100) * 100 AS qty_bucket,
    DATE_FORMAT(moved_at_date, 'MMM yyyy') AS moved_at_date_label
FROM qualified
ORDER BY move_type DESC
