SELECT p.id, p.category, p.price, s.supplier_name FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id WHERE s.country IN ('SG', 'JP', 'US') OR ARRAY_CONTAINS(p.tags, 'bestseller')
