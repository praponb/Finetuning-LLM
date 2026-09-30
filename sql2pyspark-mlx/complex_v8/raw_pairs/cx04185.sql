WITH outbound_by_sku AS (
    -- OUTBOUND rows per sku_id
    SELECT sku_id, COUNT(*) AS outbound_count, SUM(qty) AS outbound_total_qty
    FROM stock_moves
    WHERE move_type = 'OUTBOUND'
    GROUP BY sku_id
),
transfer_by_sku AS (
    -- TRANSFER rows per sku_id
    SELECT sku_id, COUNT(*) AS transfer_count, SUM(qty) AS transfer_total_qty
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
    GROUP BY sku_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.sku_id, b.sku_id) AS sku_id,
    COALESCE(a.outbound_count, 0) AS outbound_count,
    COALESCE(a.outbound_total_qty, 0) AS outbound_total_qty,
    COALESCE(b.transfer_count, 0) AS transfer_count,
    COALESCE(b.transfer_total_qty, 0) AS transfer_total_qty,
    COALESCE(a.outbound_count, 0) - COALESCE(b.transfer_count, 0) AS outbound_count_minus_transfer_count
FROM outbound_by_sku a
FULL OUTER JOIN transfer_by_sku b
  ON a.sku_id = b.sku_id
ORDER BY sku_id
