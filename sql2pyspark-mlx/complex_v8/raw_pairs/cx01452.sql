WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        CASE WHEN move_type IN ('ADJUST', 'OUTBOUND') THEN 'group_a' WHEN move_type = 'INBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price,
        ABS(qty - 239) AS qty_distance
    FROM stock_moves
    WHERE operator <> 'op_eve'
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) AS move_count,
        SUM(qty) AS total_move_qty,
        COUNT(*) AS move_count_2
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        st.total_move_qty,
        COALESCE(st.move_count_2, 0) AS move_count_2
    FROM valid_moves s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        operator,
        move_type_group,
        qty_per_unit_price,
        qty_distance,
        move_count,
        total_move_qty,
        move_count_2,
        ROUND(move_count_2 / NULLIF(SUM(move_count_2) OVER (PARTITION BY move_count_2, warehouse_id), 0), 4) AS move_count_2_share_of_move_count_2_warehouse_id,
        ROUND(qty_per_unit_price / NULLIF(SUM(qty_per_unit_price) OVER (PARTITION BY move_count_2, warehouse_id), 0), 4) AS qty_per_unit_price_share_of_move_count_2_warehou,
        LEAD(qty_distance) OVER (ORDER BY total_move_qty DESC, id) AS next_qty_distance,
        LAST_VALUE(move_count_2) IGNORE NULLS OVER (ORDER BY total_move_qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_move_count_2,
        FIRST_VALUE(move_count) OVER (ORDER BY move_count NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_move_count,
        ROW_NUMBER() OVER (ORDER BY total_move_qty DESC, id) AS seq_no
    FROM summarized_rows
),
per_group AS (
    -- Summarise per group
    SELECT
        move_count,
        warehouse_id,
        MIN(next_qty_distance) AS min_next_qty_distance,
        ROUND(100 * COUNT(CASE WHEN operator = 'op_eve' THEN 1 END) / COUNT(*), 1) AS pct_operator_op_eve
    FROM metrics
    GROUP BY 1, 2
    HAVING AVG(qty_per_unit_price_share_of_move_count_2_warehou) > 0.2
)
SELECT
    move_count,
    warehouse_id,
    min_next_qty_distance,
    FLOOR(min_next_qty_distance / 100) * 100 AS min_next_qty_distance_bucket
FROM per_group
ORDER BY warehouse_id DESC, min_next_qty_distance DESC
