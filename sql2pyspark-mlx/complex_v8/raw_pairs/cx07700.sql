SELECT id, unit_cost AS amount
FROM skus
WHERE category NOT IN ('Food', 'Apparel')
UNION
SELECT qty AS id, unit_price AS amount
FROM stock_moves
ORDER BY amount DESC, id
