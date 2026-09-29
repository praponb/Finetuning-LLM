SELECT id, price, EXPLODE(tags) AS tag_name FROM products WHERE category IN ('Home', 'Fashion', 'Electronics')
