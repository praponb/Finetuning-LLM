SELECT p.id, p.price, p.cost FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE city IN ('Chiang Mai', 'Berlin', 'Tokyo')) ORDER BY p.id DESC;
