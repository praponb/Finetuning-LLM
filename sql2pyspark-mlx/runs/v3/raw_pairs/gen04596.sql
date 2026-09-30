SELECT p.id, p.category, p.product_name, p.stock_qty FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE city = 'Tokyo') ORDER BY id DESC
