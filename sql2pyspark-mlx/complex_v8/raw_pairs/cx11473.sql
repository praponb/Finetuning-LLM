SELECT id AS ref_id, category AS name
FROM skus
EXCEPT
SELECT id, move_type
FROM stock_moves
