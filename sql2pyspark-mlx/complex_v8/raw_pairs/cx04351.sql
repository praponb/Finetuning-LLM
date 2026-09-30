SELECT id AS qty, wh_code AS label
FROM warehouses
WHERE city = 'Singapore'
UNION
SELECT qty, move_type AS label
FROM stock_moves
WHERE operator IN ('op_chan', 'op_bee', 'op_eve')
ORDER BY label
