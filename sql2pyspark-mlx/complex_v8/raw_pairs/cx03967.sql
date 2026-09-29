SELECT warehouse_id, qty, operator AS name
FROM stock_moves
UNION ALL
SELECT warehouse_id, id AS qty, operator AS name
FROM stock_moves
