SELECT products.id, products.category, products.product_name, products.cost FROM products WHERE products.category = 'Home' AND products.supplier_id IN (SELECT id FROM suppliers WHERE rating >= 2.6);
