WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        ABS(qty - 314) AS qty_distance,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE unit_price BETWEEN 58.6 AND 416.0
      AND move_type IN ('ADJUST', 'OUTBOUND')
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_warehouse_id, 0), 4) AS unit_price_share_of_warehouse_id
    FROM base
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id)
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM with_windows s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.move_type = 'OUTBOUND'
        )
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM screened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.qty <= 349
              AND st.moved_at >= TIMESTAMP '2025-01-13 12:00:00'
        )
)
-- Subtotals with GROUPING SETS
SELECT
    warehouse_id,
    operator,
    sku_id,
    ROUND(STDDEV(unit_price_share_of_warehouse_id), 2) AS stddev_unit_price_share_of_warehouse_id
FROM qualified
GROUP BY GROUPING SETS ((warehouse_id, operator, sku_id), (warehouse_id), ())
ORDER BY warehouse_id NULLS LAST, operator NULLS LAST, sku_id NULLS LAST
