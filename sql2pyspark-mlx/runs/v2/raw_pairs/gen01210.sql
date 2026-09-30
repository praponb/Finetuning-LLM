SELECT id, product_name, EXPLODE(tags) AS label FROM products WHERE category NOT IN ('Fashion', 'Home')
