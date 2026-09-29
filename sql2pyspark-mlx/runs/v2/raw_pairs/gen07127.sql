SELECT p.id, p.price, p.stock_qty, p.product_name FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating > 3.8) ORDER BY p.product_name, id DESC
