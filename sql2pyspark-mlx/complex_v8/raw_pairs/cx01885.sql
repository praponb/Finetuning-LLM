WITH prepared_moves AS (
    -- Scope the stock_moves rows
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
    WHERE move_type <> 'OUTBOUND'
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(unit_price * 1.07, 2) AS unit_price_adj,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label
    FROM prepared_moves
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        operator,
        unit_price,
        unit_price_adj,
        move_type_prefix,
        moved_at_label,
        MAX(unit_price) OVER (PARTITION BY move_type, warehouse_id) AS move_type_warehouse_id_max_unit_price,
        LAST_VALUE(qty) OVER (ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        LAG(unit_price) OVER (ORDER BY moved_at ASC, id) AS prev_unit_price,
        LAST_VALUE(unit_price) OVER (ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_price,
        LAST_VALUE(qty) IGNORE NULLS OVER (ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty
    FROM annotated
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, ROUND(AVG(unit_price), 2) AS avg_move_unit_price
    FROM stock_moves
    GROUP BY warehouse_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.avg_move_unit_price
    FROM ranked s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
)
SELECT id, metric_name, value
FROM (
    SELECT id, move_type_warehouse_id_max_unit_price, unit_price, avg_move_unit_price, last_unit_price
    FROM with_stats
) src
UNPIVOT (
    value FOR metric_name IN (move_type_warehouse_id_max_unit_price, unit_price, avg_move_unit_price, last_unit_price)
) unp
ORDER BY id, metric_name
