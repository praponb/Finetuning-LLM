SELECT products.id, products.brand, products.stock_qty FROM products WHERE products.id IN (SELECT product_id FROM inventory WHERE quantity >= 108) ORDER BY id DESC
