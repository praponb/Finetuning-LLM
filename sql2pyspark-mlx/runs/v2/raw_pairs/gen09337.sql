SELECT p.id, p.stock_qty, p.category, p.product_name FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE city IN ('London', 'New York')) ORDER BY id DESC LIMIT 20;
