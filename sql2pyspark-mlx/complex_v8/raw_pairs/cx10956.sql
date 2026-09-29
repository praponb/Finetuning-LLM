WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
),
detail_rows AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        st.id AS move_id,
        st.sku_id AS move_sku_id,
        st.unit_price AS move_unit_price,
        st.move_type AS move_move_type
    FROM base_moves s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.move_type IS NOT NULL
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM detail_rows s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.operator NOT IN ('op_dew', 'op_eve')
        )
      AND s.moved_at < TIMESTAMP '2025-03-02 13:00:00'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        move_move_type,
        warehouse_id,
        MIN(moved_at) AS first_moved_at,
        ROUND(AVG(move_unit_price), 2) AS avg_move_unit_price,
        ROUND(SUM(move_unit_price), 2) AS total_move_unit_price,
        COUNT(*) AS row_count
    FROM screened
    WHERE move_sku_id IS NOT NULL
    GROUP BY move_move_type, warehouse_id
),
screened_2 AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM stats s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE MONTH(st.moved_at) IN (2, 3)
        )
      AND s.total_move_unit_price >= 1050.7
),
enriched_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.move_move_type,
        s.warehouse_id,
        s.first_moved_at,
        s.avg_move_unit_price,
        s.row_count,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on
    FROM screened_2 s
    JOIN warehouses w ON w.id = s.warehouse_id
)
-- Final projection
SELECT
    move_move_type,
    warehouse_id,
    first_moved_at,
    avg_move_unit_price,
    row_count,
    warehouse_opened_on,
    CASE WHEN move_move_type IN ('TRANSFER', 'ADJUST') THEN 'group_a' WHEN move_move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_move_type_group
FROM enriched_warehouse
ORDER BY warehouse_opened_on DESC NULLS FIRST, move_move_type_group DESC
