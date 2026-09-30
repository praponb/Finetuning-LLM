SELECT qty AS entity_id
FROM stock_moves
EXCEPT ALL
SELECT id
FROM skus
ORDER BY entity_id
