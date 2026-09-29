SELECT s.id, s.city, s.rating, s.supplier_name FROM suppliers s WHERE s.rating >= 2.7 AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.stock_qty != 272) ORDER BY s.id;
