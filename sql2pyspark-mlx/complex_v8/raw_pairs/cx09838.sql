WITH outbound_by_sku AS (
    -- OUTBOUND rows per sku_id
    SELECT sku_id, COUNT(*) AS outbound_count, SUM(qty) AS outbound_total_qty
    FROM stock_moves
    WHERE move_type = 'OUTBOUND'
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
    COALESCE(a.outbound_count, 0) AS outbound_count,
    a.outbound_total_qty,
    COALESCE(b.inbound_count, 0) AS inbound_count,
    b.inbound_total_qty,
    COALESCE(a.outbound_count, 0) - COALESCE(b.inbound_count, 0) AS outbound_count_minus_inbound_count
FROM outbound_by_sku a
FULL OUTER JOIN inbound_by_sku b
  ON a.sku_id = b.sku_id
ORDER BY sku_id
