WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
    WHERE move_type = 'ADJUST'
      AND qty BETWEEN 61 AND 326
),
deduped AS (
    -- Keep the earliest row per sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        days_since_moved_at,
        operator_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY days_since_moved_at, id) AS row_num
        FROM scoped_moves
    ) ranked
    WHERE row_num = 1
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND MONTH(st.moved_at) IN (2, 3)
        )
),
move_stats AS (
    -- Per-warehouse stock_moves metrics
    SELECT
        warehouse_id,
        MIN(moved_at) AS first_moved_at,
        COUNT(CASE WHEN move_type <> 'ADJUST' THEN 1 END) AS move_move_type_count
    FROM stock_moves
    GROUP BY warehouse_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.first_moved_at,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count
    FROM with_activity s
    LEFT JOIN move_stats st ON st.warehouse_id = s.warehouse_id
)
SELECT id, measure, value
FROM (
    SELECT id, qty, days_since_moved_at
    FROM with_counts
) src
UNPIVOT (
    value FOR measure IN (qty, days_since_moved_at)
) unp
