SELECT id, product_name, price, EXPLODE(tags) AS tag_name FROM products WHERE cost <= 620;
