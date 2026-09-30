SELECT brand, product_name FROM products WHERE stock_qty < 102 EXCEPT SELECT brand, product_name FROM products WHERE category NOT IN ('Books', 'Fashion');
