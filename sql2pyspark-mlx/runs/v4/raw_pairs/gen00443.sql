SELECT id, price, brand, EXPLODE(tags) AS tag_name FROM products WHERE price >= 820
