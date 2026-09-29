SELECT id, launch_date, REPLACE(product_name, ' ', '') product_name_slug FROM products WHERE price BETWEEN 550 AND 630 AND category IN ('Sports', 'Books');
