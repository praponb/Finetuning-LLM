SELECT products.id, products.cost, products.stock_qty, products.supplier_id FROM products WHERE products.supplier_id NOT IN (SELECT id FROM suppliers WHERE rating BETWEEN 2.1 AND 3.2)
