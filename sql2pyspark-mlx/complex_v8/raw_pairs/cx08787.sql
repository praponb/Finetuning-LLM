SELECT id
FROM skus
WHERE category <> 'Toys'
  AND unit_cost >= 158.7
INTERSECT
SELECT id
FROM stock_moves
ORDER BY id DESC
