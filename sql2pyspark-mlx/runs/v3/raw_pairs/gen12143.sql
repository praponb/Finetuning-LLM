SELECT id, price, launch_date, LOWER(product_name) AS product_name_lower, REPLACE(product_name, ' ', '') AS product_name_slug FROM products WHERE cost <= 260 AND ARRAY_CONTAINS(tags, 'bestseller')
