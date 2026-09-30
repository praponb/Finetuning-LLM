SELECT id, product_name, REPLACE(product_name, ' ', '-') AS product_name_slug, ARRAY_JOIN(tags, ', ') AS tags_text FROM products WHERE brand = 'Initech' AND brand IS NOT NULL;
