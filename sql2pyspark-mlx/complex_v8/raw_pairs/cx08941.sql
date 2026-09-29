SELECT id AS qty, move_type
FROM stock_moves
WHERE unit_price BETWEEN 112.8 AND 330.9
INTERSECT
SELECT id AS qty, category AS move_type
FROM skus
