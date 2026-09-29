WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        MONTH(moved_at) AS moved_at_month,
        unit_price - qty AS unit_price_minus_qty
    FROM stock_moves
    WHERE qty < 370
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM base_moves s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
        )
      AND s.operator <> 'op_bee'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(unit_price_minus_qty, moved_at_month) AS least_unit_price_minus_qty_moved_at_month,
        CASE WHEN unit_price >= 328.3 THEN 'A' WHEN unit_price >= 200.7 THEN 'B' ELSE 'C' END AS unit_price_tier,
        unit_price + unit_price_minus_qty AS unit_price_plus_unit_price_minus_qty
    FROM qualified
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY unit_price DESC, id) AS rn,
        FIRST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY least_unit_price_minus_qty_moved_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty,
        NTILE(3) OVER (PARTITION BY move_type ORDER BY least_unit_price_minus_qty_moved_at_month, id) AS least_unit_price_minus_qty_moved_at_month_tercil,
        COUNT(*) OVER w_warehouse_id_move_type AS rows_in_warehouse_id_move_type,
        MIN(qty) OVER (PARTITION BY move_type ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_qty_so_far,
        NTH_VALUE(moved_at_month, 2) OVER (PARTITION BY move_type ORDER BY least_unit_price_minus_qty_moved_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at_month
    FROM labeled
    WINDOW w_warehouse_id_move_type AS (PARTITION BY warehouse_id, move_type)
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    qty,
    operator,
    unit_price,
    moved_at_month,
    unit_price_minus_qty,
    first_known_qty,
    least_unit_price_minus_qty_moved_at_month_tercil,
    min_qty_so_far,
    second_moved_at_month,
    LENGTH(TRIM(operator)) AS operator_len
FROM with_trends
WHERE operator IS NOT NULL
ORDER BY first_known_qty
