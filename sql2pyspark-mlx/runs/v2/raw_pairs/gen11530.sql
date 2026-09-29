SELECT id, tags FROM products WHERE ARRAY_CONTAINS(tags, 'clearance') AND brand IN ('Acme', 'Globex');
