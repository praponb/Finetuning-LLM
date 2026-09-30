WITH outbound_by_sku AS (
    -- OUTBOUND rows per sku_id
    SELECT sku_id, COUNT(*) AS outbound_count, SUM(qty) AS outbound_total_qty
    FROM stock_moves
    WHERE move_type = 'OUTBOUND'
    GROUP BY sku_id
),
transfer_by_sku AS (
    -- TRANSFER rows per sku_id
    SELECT sku_id, COUNT(*) AS transfer_count, ROUND(AVG(qty), 2) AS transfer_avg_qty
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
    GROUP BY sku_id
)
SELECT
    COALESCE(a.sku_id, b.sku_id) AS sku_id,
    COALESCE(a.outbound_count, 0) AS outbound_count,
    COALESCE(a.outbound_total_qty, 0) AS outbound_total_qty,
    COALESCE(b.transfer_count, 0) AS transfer_count,
    b.transfer_avg_qty,
    COALESCE(a.outbound_count, 0) - COALESCE(b.transfer_count, 0) AS net_count
FROM outbound_by_sku a
FULL OUTER JOIN transfer_by_sku b
  ON a.sku_id = b.sku_id
ORDER BY sku_id
