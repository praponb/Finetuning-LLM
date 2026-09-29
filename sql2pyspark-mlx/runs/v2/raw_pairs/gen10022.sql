SELECT id, brand, EXPLODE(tags) AS tag_name FROM products WHERE product_name LIKE '%Lapt%'
