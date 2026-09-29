SELECT supplier_id FROM products WHERE category = 'Books' INTERSECT SELECT supplier_id FROM products WHERE ARRAY_CONTAINS(tags, 'sale')
