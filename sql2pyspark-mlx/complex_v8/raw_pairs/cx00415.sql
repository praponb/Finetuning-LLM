WITH valid_moves AS (
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
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM valid_moves s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
        )
      AND TO_DATE(s.moved_at) < DATE '2025-03-02'
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        qty_per_unit_price,
        CASE WHEN qty_per_unit_price >= 7.5 THEN 'HIGH' WHEN qty_per_unit_price >= 4.0 THEN 'MEDIUM' ELSE 'LOW' END AS qty_per_unit_price_tier
    FROM without_matches
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    moved_at,
    operator,
    unit_price,
    qty_per_unit_price,
    qty_per_unit_price_tier
FROM labeled
WHERE move_type = 'INBOUND'
ORDER BY id DESC, operator
