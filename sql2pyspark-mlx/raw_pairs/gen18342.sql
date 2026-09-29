SELECT id, price FROM products WHERE brand IN ('Globex', 'Acme') AND ARRAY_CONTAINS(tags, 'eco') ORDER BY id, price DESC;
