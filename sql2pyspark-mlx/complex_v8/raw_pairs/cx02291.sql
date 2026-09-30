WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        ABS(unit_price - 161.4) AS unit_price_distance,
        CASE WHEN qty >= 415 THEN 'A' WHEN qty >= 137 THEN 'B' ELSE 'C' END AS qty_tier,
        YEAR(moved_at) AS moved_at_year
    FROM stock_moves
    WHERE operator IN ('op_chan', 'op_eve', 'op_dew')
      AND warehouse_id <= 2
),
combined_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.unit_price_distance,
        s.qty_tier,
        s.moved_at_year,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity
    FROM cleaned s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM combined_details_warehouse s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.unit_price < 307.8
        )
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
        )
)
SELECT *
FROM (
    SELECT sku_id, move_type, qty
    FROM without_matches
) src
PIVOT (
    AVG(qty) AS avg_v, MAX(qty) AS max_v
    FOR move_type IN ('ADJUST', 'TRANSFER', 'INBOUND', 'OUTBOUND')
)
