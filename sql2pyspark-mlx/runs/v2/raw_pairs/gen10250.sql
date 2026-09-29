SELECT id, brand, product_name FROM products WHERE YEAR(launch_date) = 2025 INTERSECT SELECT id, brand, product_name FROM products WHERE SIZE(tags) > 1 ORDER BY id;
