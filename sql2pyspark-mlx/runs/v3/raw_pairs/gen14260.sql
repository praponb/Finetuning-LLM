SELECT product_name, category FROM products WHERE brand != 'Initech' EXCEPT SELECT product_name, category FROM products WHERE stock_qty IS NULL ORDER BY category
