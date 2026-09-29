SELECT id, category, EXPLODE(tags) AS label FROM products WHERE product_name IS NULL;
