SELECT qty, unit_price
FROM stock_moves
UNION ALL
SELECT id AS qty, unit_cost AS unit_price
FROM skus
WHERE is_active = TRUE
