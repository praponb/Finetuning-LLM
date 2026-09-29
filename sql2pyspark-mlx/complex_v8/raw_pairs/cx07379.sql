SELECT 'move' AS source, qty AS ref_id, id AS key_id
FROM stock_moves
WHERE TO_DATE(moved_at) < DATE '2025-02-12'
UNION ALL
SELECT 'move_2' AS source, qty AS ref_id, sku_id AS key_id
FROM stock_moves
WHERE warehouse_id IS NOT NULL
