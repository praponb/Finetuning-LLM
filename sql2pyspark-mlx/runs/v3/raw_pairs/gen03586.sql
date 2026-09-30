SELECT s.id, s.city, s.rating, s.supplier_name FROM suppliers s WHERE s.country = 'UK' AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND ARRAY_CONTAINS(p.tags, 'sale'))
