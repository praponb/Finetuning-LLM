SELECT id AS qty
FROM skus
WHERE category <> 'Toys'
EXCEPT ALL
SELECT id
FROM skus
ORDER BY qty
