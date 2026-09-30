WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd') AS moved_at_label,
        CASE WHEN unit_price >= 361.9 THEN 'large' WHEN unit_price >= 125.9 THEN 'medium' ELSE 'small' END AS unit_price_tier
    FROM stock_moves
    WHERE operator = 'op_chan'
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM filtered s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.move_type IN ('OUTBOUND', 'TRANSFER')
              AND st.qty <= 355
        )
),
without_matches AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND w.opened_on BETWEEN DATE '2024-02-19' AND DATE '2025-07-03'
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, value
FROM (
    SELECT id, unit_price_bucket, qty
    FROM without_matches
) src
UNPIVOT (
    value FOR measure IN (unit_price_bucket, qty)
) unp
ORDER BY id, measure
