WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        LENGTH(TRIM(operator)) AS operator_len
    FROM stock_moves
    WHERE sku_id <= 7
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY warehouse_id, move_type)) AS distinct_sku_id_in_warehouse_id_move_type,
        ROUND(operator_len - AVG(operator_len) OVER w_warehouse_id_move_type, 2) AS operator_len_vs_warehouse_id_move_type_avg
    FROM prepared
    WINDOW w_warehouse_id_move_type AS (PARTITION BY warehouse_id, move_type)
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(CASE WHEN move_type <> 'ADJUST' THEN 1 END) AS move_move_type_count,
        ROUND(AVG(unit_price), 2) AS avg_move_unit_price,
        MAX(qty) AS max_move_qty
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count,
        st.avg_move_unit_price,
        st.max_move_qty
    FROM ranked s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
)
SELECT
    operator_len,
    operator,
    warehouse_id,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT operator) AS distinct_operators,
    COUNT(DISTINCT move_type) AS distinct_move_types
FROM profiled
GROUP BY CUBE(operator_len, operator, warehouse_id)
