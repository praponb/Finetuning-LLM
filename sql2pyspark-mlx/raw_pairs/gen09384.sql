SELECT *, TRIM(product_name) AS clean_product_name FROM products WHERE price BETWEEN 800 AND 1130 OR MONTH(launch_date) IN (2, 3, 5);
