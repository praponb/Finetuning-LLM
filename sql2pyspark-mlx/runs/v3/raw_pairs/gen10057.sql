SELECT id, product_name, EXPLODE(tags) AS label FROM products WHERE product_name LIKE '%Hub%'
