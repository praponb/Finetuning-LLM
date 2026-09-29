SELECT id AS ref_id
FROM skus
WHERE category = 'Toys'
MINUS
SELECT id
FROM skus
