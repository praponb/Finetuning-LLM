WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour,
        qty - unit_price AS qty_minus_unit_price
    FROM stock_moves
    WHERE operator = 'op_ann'
      AND HOUR(moved_at) BETWEEN 6 AND 12
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        unit_price + qty AS unit_price_plus_qty,
        TO_DATE(moved_at) AS moved_at_date
    FROM scoped_moves
    WHERE qty <= 393
),
labeled_2 AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(unit_price * 1.5, 2) AS unit_price_adj,
        COALESCE(qty, 0) AS qty_filled,
        CONCAT(operator, '#', CAST(warehouse_id AS STRING)) AS operator_ref
    FROM labeled
    WHERE moved_at_date IS NOT NULL
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        s.moved_at_hour,
        s.qty_minus_unit_price,
        s.unit_price_plus_qty,
        s.unit_price_adj,
        st.id AS move_id,
        st.warehouse_id AS move_warehouse_id,
        st.qty AS move_qty,
        st.operator AS move_operator,
        st.moved_at AS move_moved_at
    FROM labeled_2 s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM line_level
    WHERE unit_price_plus_qty > (SELECT AVG(unit_price_plus_qty) FROM line_level)
),
screened_2 AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM screened s
    WHERE s.sku_id IN (
            SELECT st.sku_id
            FROM stock_moves st
            WHERE st.operator = 'op_dew'
        )
),
warehouse_id_stats AS (
    -- Avg qty_minus_unit_price per warehouse_id
    SELECT warehouse_id, AVG(qty_minus_unit_price) AS avg_qty_minus_unit_price
    FROM screened_2
    GROUP BY warehouse_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_qty_minus_unit_price,
        ROUND(s.qty_minus_unit_price - g.avg_qty_minus_unit_price, 2) AS qty_minus_unit_price_minus_avg
    FROM screened_2 s
    JOIN warehouse_id_stats g ON g.warehouse_id = s.warehouse_id
    WHERE s.qty_minus_unit_price > g.avg_qty_minus_unit_price
)
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    unit_price,
    qty_minus_unit_price,
    unit_price_plus_qty,
    unit_price_adj,
    move_operator,
    move_moved_at,
    avg_qty_minus_unit_price,
    qty_minus_unit_price_minus_avg,
    DATE_TRUNC('DAY', moved_at) AS moved_at_day,
    DAYOFWEEK(move_moved_at) AS move_moved_at_dayofweek
FROM vs_group
ORDER BY unit_price
