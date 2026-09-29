WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty,
        CONCAT_WS('|', move_type, operator) AS move_type_operator_label
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-03-02 09:00:00'
),
derived AS (
    -- Derive helper columns
    SELECT *, ABS(unit_price_per_qty - 4.5) AS unit_price_per_qty_distance
    FROM prepared_moves
    WHERE unit_price_per_qty > 1.5
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND HOUR(st.moved_at) BETWEEN 6 AND 11
              AND st.sku_id IS NOT NULL
        )
),
deduped AS (
    -- Keep the earliest row per operator, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        unit_price_per_qty,
        move_type_operator_label,
        unit_price_per_qty_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, sku_id ORDER BY unit_price_per_qty, id) AS row_num
        FROM qualified
    ) ranked
    WHERE row_num = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT moved_at, sku_id, operator, unit_price_per_qty
    FROM deduped
) src
PIVOT (
    MIN(unit_price_per_qty)
    FOR operator IN ('op_ann', 'op_bee', 'op_chan')
)
