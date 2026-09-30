SELECT id, brand, EXPLODE(tags) AS label FROM products WHERE ARRAY_CONTAINS(tags, 'new')
