SELECT id, price, product_name, brand FROM products WHERE price >= (SELECT MAX(price) FROM products WHERE stock_qty >= 341) ORDER BY product_name DESC, id LIMIT 10
