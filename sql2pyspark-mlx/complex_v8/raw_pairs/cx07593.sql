WITH scoped AS (
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
        CASE WHEN qty >= 288 THEN 'HIGH' WHEN qty >= 191 THEN 'MEDIUM' ELSE 'LOW' END AS qty_tier,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label,
        CONCAT_WS('-', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE qty <= 348
),
move_type_stats AS (
    -- Max unit_price per move_type
    SELECT move_type, MAX(unit_price) AS max_unit_price
    FROM scoped
    GROUP BY move_type
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_unit_price,
        ROUND(s.unit_price - g.max_unit_price, 2) AS unit_price_minus_max
    FROM scoped s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.unit_price = g.max_unit_price
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM above_average s
    WHERE s.sku_id IN (
            SELECT st.sku_id
            FROM stock_moves st
        )
      AND s.unit_price_minus_max < 400.0
),
tagged AS (
    -- Derive helper columns
    SELECT *, unit_price + qty AS unit_price_plus_qty
    FROM with_activity
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    operator,
    unit_price,
    qty_tier,
    moved_at_label,
    max_unit_price,
    unit_price_minus_max,
    unit_price_plus_qty,
    SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix,
    CASE WHEN operator IN ('op_bee', 'op_eve') THEN 'group_a' WHEN operator = 'op_chan' THEN 'group_b' ELSE 'other' END AS operator_group
FROM tagged
WHERE operator IS NOT NULL
