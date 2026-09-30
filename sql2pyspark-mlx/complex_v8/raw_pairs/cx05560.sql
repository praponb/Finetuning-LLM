WITH cleaned AS (
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
        LENGTH(TRIM(move_type)) AS move_type_len
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        ABS(unit_price - 164.1) AS unit_price_distance,
        LENGTH(TRIM(move_type)) AS move_type_len_2
    FROM cleaned
    WHERE unit_price > 160.8
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM labeled s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY move_type_len_2 ORDER BY qty DESC, id) AS qty_rank,
        RANK() OVER (PARTITION BY move_type_len_2 ORDER BY qty DESC) AS qty_rank_2,
        MAX(unit_price_distance) OVER (PARTITION BY warehouse_id, operator) AS warehouse_id_operator_max_unit_price_distance,
        LAG(unit_price_distance, 1, 0) OVER (PARTITION BY move_type_len_2 ORDER BY qty DESC, id) AS prev_unit_price_distance,
        COUNT(*) OVER w_warehouse_id_operator AS rows_in_warehouse_id_operator
    FROM with_stats
    WINDOW w_move_type_len_2_by_qty_ties AS (PARTITION BY move_type_len_2 ORDER BY qty DESC), w_warehouse_id_operator AS (PARTITION BY warehouse_id, operator)
),
top_rows AS (
    -- Top 3 per group
    SELECT *
    FROM windowed
    WHERE qty_rank <= 3
      AND move_type IN ('OUTBOUND', 'ADJUST', 'TRANSFER')
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at,
    operator,
    unit_price_distance,
    move_type_len_2,
    move_count,
    qty_rank_2,
    warehouse_id_operator_max_unit_price_distance,
    prev_unit_price_distance,
    rows_in_warehouse_id_operator,
    LOWER(TRIM(operator)) AS operator_clean,
    COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
FROM top_rows
WHERE move_type IN ('TRANSFER', 'OUTBOUND')
ORDER BY operator DESC, prev_unit_price_distance DESC, id
LIMIT 10
