SELECT id, product_name, category, EXPLODE(tags) AS tag FROM products WHERE stock_qty BETWEEN 201 AND 347;
