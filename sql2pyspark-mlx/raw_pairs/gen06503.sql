SELECT p.id, p.stock_qty, p.price FROM products p WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE rating > 2.6) ORDER BY price, id LIMIT 5;
