SELECT id, supplier_id FROM products WHERE supplier_id IS NOT NULL INTERSECT SELECT id, supplier_id FROM products WHERE brand IS NOT NULL ORDER BY supplier_id
