SELECT category, id AS key_id
FROM skus
UNION ALL
SELECT move_type AS category, qty AS key_id
FROM stock_moves
ORDER BY category DESC
