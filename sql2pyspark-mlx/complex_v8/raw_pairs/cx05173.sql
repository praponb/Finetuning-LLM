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
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour
    FROM stock_moves
    WHERE operator = 'op_bee'
      AND unit_price <= 387.1
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.moved_at_hour,
        st.id AS move_id,
        st.move_type AS move_move_type,
        st.operator AS move_operator,
        st.moved_at AS move_moved_at,
        st.qty AS move_qty
    FROM filtered_moves s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.move_type = 'INBOUND'
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.move_id,
        s.move_operator,
        s.move_moved_at,
        s.move_qty,
        st.id AS move_id_2,
        st.operator AS move_operator_2,
        st.moved_at AS move_moved_at_2,
        st.unit_price AS move_unit_price,
        st.qty AS move_qty_2
    FROM line_level_moves s
    JOIN stock_moves st ON st.sku_id = s.sku_id AND st.operator = 'op_eve'
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM with_children_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.operator <> 'op_bee'
              AND st.qty > 148
        )
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', move_moved_at) AS move_moved_at_month,
        SUM(move_unit_price) AS total_move_unit_price,
        COUNT(*) FILTER (WHERE operator = 'op_dew') AS n_operator_op_dew
    FROM with_activity
    GROUP BY DATE_TRUNC('MONTH', move_moved_at)
),
enriched AS (
    -- Add calculated columns
    SELECT
        move_moved_at_month,
        total_move_unit_price,
        n_operator_op_dew,
        ROUND(n_operator_op_dew / NULLIF(total_move_unit_price, 0), 3) AS n_operator_op_dew_per_total_move_unit_price,
        COALESCE(n_operator_op_dew, 0) AS n_operator_op_dew_filled,
        DATE_TRUNC('DAY', move_moved_at_month) AS move_moved_at_month_day
    FROM per_group
)
SELECT
    move_moved_at_month,
    total_move_unit_price,
    n_operator_op_dew,
    n_operator_op_dew_per_total_move_unit_price,
    move_moved_at_month_day,
    DATE_TRUNC('WEEK', move_moved_at_month) AS move_moved_at_month_week
FROM enriched
WHERE n_operator_op_dew >= 2
ORDER BY total_move_unit_price
