WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        INITCAP(TRIM(move_type)) AS move_type_clean
    FROM stock_moves
    WHERE operator = 'op_eve'
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.qty > 116
        )
)
SELECT
    COALESCE(move_type, 'ALL') AS move_type,
    warehouse_id,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(qty_bucket) / NULLIF(SUM(qty), 0), 3) AS qty_bucket_to_qty_ratio,
    ROUND(AVG(qty), 2) AS avg_qty,
    COUNT(*) AS row_count
FROM with_activity
GROUP BY CUBE(move_type, warehouse_id)
ORDER BY move_type NULLS LAST, warehouse_id NULLS LAST
