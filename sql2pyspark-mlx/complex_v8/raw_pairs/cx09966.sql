WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price,
        FLOOR(qty / 100) * 100 AS qty_bucket,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month
    FROM stock_moves
    WHERE unit_price > 95.8
),
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM filtered s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
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
              AND st.move_type = 'OUTBOUND'
        )
      AND s.moved_at >= TIMESTAMP '2025-02-12 10:00:00'
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, qty, qty_bucket
    FROM qualified
) src
UNPIVOT (
    reading FOR metric_name IN (qty AS m_qty, qty_bucket AS qty_bucket_v)
) unp
