WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        LENGTH(TRIM(move_type)) AS move_type_len,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
      AND unit_price >= 61.2
),
enriched AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.move_type_len,
        s.moved_at_date,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on
    FROM filtered_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    WHERE w.opened_on >= DATE '2024-04-13'
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.unit_price < 292.3
        )
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS max_move_qty,
        (SELECT SUM(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS total_move_qty
    FROM filtered s
),
with_parent AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at_date,
        s.warehouse_capacity,
        s.warehouse_opened_on,
        sk.sku_code
    FROM profiled s
    JOIN skus sk ON sk.id = s.sku_id
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_ADD(warehouse_opened_on, 30) AS warehouse_opened_on_plus_30d,
        CASE WHEN warehouse_capacity >= 11506 THEN 'HIGH' WHEN warehouse_capacity >= 11005 THEN 'MEDIUM' ELSE 'LOW' END AS warehouse_capacity_tier
    FROM with_parent
    WHERE warehouse_opened_on >= DATE '2024-04-13'
)
-- Subtotals with GROUPING SETS
SELECT
    warehouse_id,
    COALESCE(move_type, 'ALL') AS move_type,
    sku_id,
    COUNT(*) AS row_count
FROM enriched_2
GROUP BY GROUPING SETS ((warehouse_id, move_type, sku_id), (warehouse_id), (move_type), ())
