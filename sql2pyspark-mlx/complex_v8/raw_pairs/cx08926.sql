WITH inbound_by_sku AS (
    -- INBOUND rows per sku_id
    SELECT sku_id, COUNT(*) AS inbound_count, SUM(qty) AS inbound_total_qty
    FROM stock_moves
    WHERE move_type = 'INBOUND'
    GROUP BY sku_id
),
outbound_by_sku AS (
    -- OUTBOUND rows per sku_id
    SELECT sku_id, COUNT(*) AS outbound_count, ROUND(AVG(qty), 2) AS outbound_avg_qty
    FROM stock_moves
    WHERE move_type = 'OUTBOUND'
    GROUP BY sku_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.sku_id, b.sku_id) AS sku_id,
    COALESCE(a.inbound_count, 0) AS inbound_count,
    a.inbound_total_qty,
    COALESCE(b.outbound_count, 0) AS outbound_count,
    COALESCE(b.outbound_avg_qty, 0) AS outbound_avg_qty,
    COALESCE(a.inbound_count, 0) - COALESCE(b.outbound_count, 0) AS inbound_count_minus_outbound_count
FROM inbound_by_sku a
FULL OUTER JOIN outbound_by_sku b
  ON a.sku_id = b.sku_id
ORDER BY sku_id
