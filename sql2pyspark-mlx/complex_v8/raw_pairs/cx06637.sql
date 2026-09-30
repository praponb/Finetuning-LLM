WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE move_type NOT IN ('OUTBOUND', 'ADJUST')
      AND sku_id <= 5
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM prepared
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.unit_price <= 330.9
              AND st.qty < 275
        )
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS last_moved_at,
        (SELECT COUNT(*) FILTER (WHERE st.operator <> 'op_eve') FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_operator_count
    FROM filtered s
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    unit_price,
    days_since_moved_at,
    last_moved_at,
    move_operator_count
FROM profiled
ORDER BY days_since_moved_at DESC, move_type DESC
