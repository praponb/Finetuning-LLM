WITH valid_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity
    FROM warehouses
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.id) AS last_moved_at,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.id) AS last_moved_at_2
    FROM valid_warehouses s
),
tagged AS (
    -- Label and score rows
    SELECT *, DATE_TRUNC('WEEK', last_moved_at) AS last_moved_at_week
    FROM with_stats
    WHERE last_moved_at < TIMESTAMP '2025-02-17 09:00:00'
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.id
        )
)
SELECT *
FROM (
    SELECT last_moved_at, last_moved_at_2, city, capacity
    FROM filtered
) src
PIVOT (
    SUM(capacity)
    FOR city IN ('Singapore', 'Bangkok')
)
ORDER BY last_moved_at, last_moved_at_2
