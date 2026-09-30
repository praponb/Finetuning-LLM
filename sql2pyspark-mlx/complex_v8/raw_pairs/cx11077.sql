SELECT capacity AS qty, city AS label
FROM warehouses
WHERE city IN ('Kuala Lumpur', 'Bangkok')
UNION ALL
SELECT sku_id AS qty, operator AS label
FROM stock_moves
WHERE moved_at < TIMESTAMP '2025-02-24 12:00:00'
ORDER BY qty DESC
