SELECT s.id, s.city, s.supplier_name, s.rating FROM suppliers s WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.brand IN ('Umbrella', 'Acme')) ORDER BY s.id DESC;
