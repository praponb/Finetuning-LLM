SELECT s.id, s.rating, s.country, s.supplier_name FROM suppliers s WHERE s.country IN ('DE', 'SG') AND s.id IN (SELECT supplier_id FROM products WHERE category = 'Electronics');
