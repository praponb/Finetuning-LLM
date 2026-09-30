SELECT s.* FROM suppliers s WHERE EXISTS (SELECT 1 FROM products ch WHERE ch.supplier_id = s.id AND ch.price >= 560)
