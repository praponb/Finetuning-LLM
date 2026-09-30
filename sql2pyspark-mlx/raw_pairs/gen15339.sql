SELECT s.id, s.rating FROM suppliers s WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id) ORDER BY rating, s.id DESC;
