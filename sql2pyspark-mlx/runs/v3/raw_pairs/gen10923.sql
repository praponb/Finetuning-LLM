SELECT s.id, s.city, s.rating, s.country FROM suppliers s WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND SIZE(p.tags) > 1);
