WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
      AND operator NOT IN ('op_chan', 'op_ann')
),
deduped AS (
    -- Keep the latest row per warehouse_id, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id, move_type ORDER BY unit_price DESC, id DESC) AS recency_rank
        FROM prepared_moves
    ) ranked
    WHERE recency_rank = 1
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE st.move_type = 'TRANSFER') FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_move_type_count,
        (SELECT COUNT(*) FILTER (WHERE st.move_type = 'OUTBOUND') FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_move_type_count_2
    FROM deduped s
),
move_stats_2 AS (
    -- Per-warehouse stock_moves metrics
    SELECT warehouse_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM profiled s
    LEFT JOIN move_stats_2 st ON st.warehouse_id = s.warehouse_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT move_move_type_count_2, move_move_type_count, operator, unit_price
    FROM summarized_rows
) src
PIVOT (
    MAX(unit_price) AS max_v, COUNT(unit_price) AS n
    FOR operator IN ('op_chan', 'op_eve', 'op_ann', 'op_dew')
)
ORDER BY move_move_type_count_2, move_move_type_count
