SELECT warehouse_id AS entity_id, moved_at, move_type AS category
FROM stock_moves
WHERE operator IS NOT NULL
UNION ALL
SELECT qty AS entity_id, moved_at, move_type AS category
FROM stock_moves
WHERE unit_price BETWEEN 111.7 AND 367.7
  AND qty <= 393
UNION ALL
SELECT id AS entity_id, moved_at, move_type AS category
FROM stock_moves
