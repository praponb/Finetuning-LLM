SELECT products.id, products.supplier_id, products.cost FROM products WHERE products.supplier_id = 21 AND products.id IN (SELECT product_id FROM store_sales);
