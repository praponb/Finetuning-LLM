SELECT id, brand, product_name, EXPLODE(tags) AS tag_name FROM products WHERE SIZE(tags) >= 2;
