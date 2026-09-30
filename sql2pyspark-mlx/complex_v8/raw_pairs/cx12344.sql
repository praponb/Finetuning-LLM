WITH transfer_by_sku AS (
    -- TRANSFER rows per sku_id
    SELECT sku_id, COUNT(*) AS transfer_count, SUM(qty) AS transfer_total_qty
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
    GROUP BY sku_id
),
inbound_by_sku AS (
    -- INBOUND rows per sku_id
    SELECT sku_id, COUNT(*) AS inbound_count, SUM(qty) AS inbound_total_qty
    FROM stock_moves
    WHERE move_type = 'INBOUND'
    GROUP BY sku_id
)
SELECT
    COALESCE(a.sku_id, b.sku_id) AS sku_id,
    COALESCE(a.transfer_count, 0) AS transfer_count,
    COALESCE(a.transfer_total_qty, 0) AS transfer_total_qty,
    COALESCE(b.inbound_count, 0) AS inbound_count,
    COALESCE(b.inbound_total_qty, 0) AS inbound_total_qty,
    COALESCE(a.transfer_count, 0) - COALESCE(b.inbound_count, 0) AS transfer_count_minus_inbound_count
FROM transfer_by_sku a
FULL OUTER JOIN inbound_by_sku b
  ON a.sku_id = b.sku_id
ORDER BY sku_id
