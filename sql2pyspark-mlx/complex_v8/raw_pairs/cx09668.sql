WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 6 AND 11
),
with_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city
    FROM base s
    JOIN warehouses w ON w.id = s.warehouse_id
),
deduped AS (
    -- Keep the earliest row per operator
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        warehouse_wh_code,
        warehouse_opened_on,
        warehouse_city
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator ORDER BY qty, id) AS rn
        FROM with_details
    ) ranked
    WHERE rn = 1
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        COUNT(*) FILTER (WHERE move_type = 'TRANSFER') AS move_move_type_count,
        MAX(moved_at) AS last_moved_at
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count,
        st.last_moved_at
    FROM deduped s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT operator, warehouse_city, move_move_type_count
    FROM with_counts
) src
PIVOT (
    SUM(move_move_type_count)
    FOR warehouse_city IN ('Singapore', 'Bangkok')
)
ORDER BY operator
