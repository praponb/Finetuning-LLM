SELECT p.id, p.product_name FROM products p WHERE ARRAY_CONTAINS(p.tags, 'eco') AND p.supplier_id IN (SELECT id FROM suppliers WHERE city = 'Chiang Mai') ORDER BY p.id;
