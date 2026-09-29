WITH filtered AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        CASE WHEN capacity >= 11506 THEN 'large' WHEN capacity >= 4912 THEN 'medium' ELSE 'small' END AS capacity_tier
    FROM warehouses
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.id) AS last_moved_at
    FROM filtered s
),
move_stats_2 AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        MAX(moved_at) AS last_moved_at_2,
        COUNT(*) AS move_count,
        COUNT(*) AS move_count_2
    FROM stock_moves
    GROUP BY warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.last_moved_at_2,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_count_2, 0) AS move_count_2
    FROM with_counts s
    LEFT JOIN move_stats_2 st ON st.warehouse_id = s.id
)
SELECT id, measure, amount
FROM (
    SELECT id, move_count_2, capacity
    FROM summarized_rows
) src
UNPIVOT (
    amount FOR measure IN (move_count_2, capacity)
) unp
ORDER BY id, measure
