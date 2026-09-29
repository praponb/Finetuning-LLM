WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE unit_price BETWEEN 130.6 AND 363.6
      AND qty BETWEEN 116 AND 357
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(unit_price, qty) AS greatest_unit_price_qty,
        qty - unit_price AS qty_minus_unit_price,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix
    FROM scoped_moves
    WHERE move_type IS NOT NULL
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        ROUND(AVG(unit_price), 2) AS avg_move_unit_price,
        COUNT(*) AS move_count,
        MAX(qty) AS max_move_qty
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.avg_move_unit_price,
        COALESCE(st.move_count, 0) AS move_count,
        st.max_move_qty
    FROM enriched s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM summarized_rows s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.qty BETWEEN 89 AND 393
        )
      AND s.qty_minus_unit_price BETWEEN 200.8 AND 700.3
)
SELECT *
FROM (
    SELECT move_type, operator, qty_minus_unit_price
    FROM with_activity
) src
PIVOT (
    SUM(qty_minus_unit_price)
    FOR operator IN ('op_ann', 'op_chan', 'op_bee')
)
