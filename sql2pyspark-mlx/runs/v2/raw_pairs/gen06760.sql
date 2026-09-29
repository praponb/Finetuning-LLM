SELECT id, price, category, EXPLODE(tags) AS tag_name FROM products WHERE cost >= 610;
