SELECT id, brand, product_name, EXPLODE(tags) AS tag FROM products WHERE SIZE(tags) >= 2
