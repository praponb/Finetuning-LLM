WITH filtered_moves AS (
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
        MONTH(moved_at) AS moved_at_month
    FROM stock_moves
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN st.operator <> 'op_chan' THEN 1 END) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_operator_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count_2
    FROM filtered_moves s
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        moved_at_month,
        move_operator_count,
        move_count,
        move_count_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(moved_at), warehouse_id ORDER BY moved_at DESC, id), 3) AS moved_at_percent_rank,
        ROW_NUMBER() OVER (ORDER BY qty NULLS LAST, id) AS rn
    FROM with_counts
),
sku_id_stats AS (
    -- Max move_operator_count per sku_id
    SELECT sku_id, MAX(move_operator_count) AS max_move_operator_count
    FROM windowed
    GROUP BY sku_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_move_operator_count,
        ROUND(s.move_operator_count - g.max_move_operator_count, 2) AS move_operator_count_minus_max
    FROM windowed s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.move_operator_count = g.max_move_operator_count
)
SELECT
    id,
    sku_id,
    move_type,
    operator,
    unit_price,
    moved_at_month,
    move_count,
    moved_at_percent_rank,
    rn,
    max_move_operator_count,
    move_operator_count_minus_max,
    LEAST(moved_at_month, max_move_operator_count) AS least_moved_at_month_max_move_operator_count
FROM compared
ORDER BY moved_at_month, unit_price NULLS LAST
