SELECT id, cost FROM products WHERE cost >= (SELECT MAX(cost) FROM products WHERE supplier_id IN (13, 19, 23)) ORDER BY cost ASC;
