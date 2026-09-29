SELECT s.id, s.rating, s.country, s.city FROM suppliers s WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.stock_qty IS NOT NULL) ORDER BY city
