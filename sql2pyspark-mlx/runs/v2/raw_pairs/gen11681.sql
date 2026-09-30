SELECT id, price, brand, EXPLODE(tags) AS tag FROM products WHERE SIZE(tags) > 1
