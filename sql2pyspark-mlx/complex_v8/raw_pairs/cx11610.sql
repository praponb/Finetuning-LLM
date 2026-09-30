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
        unit_price,
        moved_at + INTERVAL 7 DAYS AS moved_at_plus_7d,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week
    FROM stock_moves
    WHERE move_type IS NOT NULL
      AND warehouse_id <= 5
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.unit_price <= 298.9
        )
)
-- Subtotals with ROLLUP
SELECT
    warehouse_id,
    sku_id,
    COALESCE(operator, 'ALL') AS operator,
    ROUND(AVG(qty), 2) AS avg_qty
FROM screened
GROUP BY ROLLUP(warehouse_id, sku_id, operator)
ORDER BY warehouse_id NULLS LAST, sku_id NULLS LAST, operator NULLS LAST
