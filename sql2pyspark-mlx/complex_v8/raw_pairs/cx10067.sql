WITH inbound_by_sku AS (
    -- INBOUND rows per sku_id
    SELECT sku_id, COUNT(*) AS inbound_count, SUM(qty) AS inbound_total_qty
    FROM stock_moves
    WHERE move_type = 'INBOUND'
    GROUP BY sku_id
),
transfer_by_sku AS (
    -- TRANSFER rows per sku_id
    SELECT sku_id, COUNT(*) AS transfer_count, SUM(qty) AS transfer_total_qty
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
    GROUP BY sku_id
)
SELECT
    COALESCE(a.sku_id, b.sku_id) AS sku_id,
    COALESCE(a.inbound_count, 0) AS inbound_count,
    a.inbound_total_qty,
    COALESCE(b.transfer_count, 0) AS transfer_count,
    b.transfer_total_qty,
    COALESCE(a.inbound_count, 0) - COALESCE(b.transfer_count, 0) AS inbound_count_minus_transfer_count
FROM inbound_by_sku a
FULL OUTER JOIN transfer_by_sku b
  ON a.sku_id = b.sku_id
ORDER BY sku_id
