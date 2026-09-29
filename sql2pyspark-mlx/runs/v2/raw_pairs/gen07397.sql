SELECT products.id, products.stock_qty, products.brand FROM products WHERE products.supplier_id IN (SELECT id FROM suppliers WHERE city NOT IN ('Chiang Mai', 'Berlin', 'London'));
