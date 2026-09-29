SELECT p.id, p.cost, p.stock_qty, p.launch_date FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE country IN ('TH', 'DE', 'JP')) ORDER BY stock_qty DESC, id LIMIT 10;
