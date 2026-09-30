SELECT id, product_name, launch_date, category, UPPER(brand) AS brand_upper FROM products WHERE product_name IS NOT NULL
