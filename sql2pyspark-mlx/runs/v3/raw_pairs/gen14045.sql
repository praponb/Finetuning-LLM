SELECT id, price FROM products WHERE price < (SELECT MIN(price) FROM products WHERE supplier_id = 27);
