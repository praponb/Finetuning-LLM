SELECT s.id, s.rating FROM suppliers s WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND SIZE(p.tags) > 2) ORDER BY s.rating DESC, id LIMIT 10;
