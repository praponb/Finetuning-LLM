WITH cleaned_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator
    FROM stock_moves
    WHERE (moved_at < TIMESTAMP '2025-02-24 12:00:00' OR operator = 'op_bee')
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM cleaned_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator = 'op_eve'
              AND st.qty >= 116
        )
)
SELECT
    sku_id,
    warehouse_id,
    SUM(qty) AS total_qty,
    COUNT(DISTINCT id) AS distinct_ids
FROM qualified
GROUP BY CUBE(sku_id, warehouse_id)
ORDER BY sku_id NULLS LAST, warehouse_id NULLS LAST
