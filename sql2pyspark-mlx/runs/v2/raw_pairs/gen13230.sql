SELECT id, launch_date, brand FROM products WHERE SIZE(tags) >= 1 AND product_name LIKE '%Hub%';
