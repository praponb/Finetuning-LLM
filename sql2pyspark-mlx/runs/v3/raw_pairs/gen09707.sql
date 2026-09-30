SELECT id, tags, cost * price AS margin
FROM products
WHERE ARRAY_CONTAINS(tags, 'clearance')
ORDER BY margin DESC, id
LIMIT 5
