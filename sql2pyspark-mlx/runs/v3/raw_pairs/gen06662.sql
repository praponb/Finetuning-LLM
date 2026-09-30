SELECT p.id, p.supplier_id FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE city NOT IN ('Singapore', 'Tokyo', 'London')) ORDER BY p.supplier_id DESC
