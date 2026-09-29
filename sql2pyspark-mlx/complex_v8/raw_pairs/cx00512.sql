SELECT id AS ref_id
FROM skus
INTERSECT ALL
SELECT qty AS ref_id
FROM stock_moves
ORDER BY ref_id
