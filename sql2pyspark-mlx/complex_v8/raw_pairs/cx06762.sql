SELECT move_type AS label, moved_at AS happened_at
FROM stock_moves
WHERE unit_price > 164.1
  AND qty >= 67
UNION ALL
SELECT operator AS label, moved_at AS happened_at
FROM stock_moves
