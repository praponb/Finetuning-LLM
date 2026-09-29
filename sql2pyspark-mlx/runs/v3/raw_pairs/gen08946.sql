SELECT id, product_name, price, EXPLODE(tags) AS label FROM products;
