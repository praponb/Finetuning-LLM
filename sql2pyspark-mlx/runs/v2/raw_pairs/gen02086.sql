SELECT tags, product_name, SUBSTRING(brand, 1, 4) brand_prefix FROM products WHERE price > 400 AND MONTH(launch_date) IN (1, 6, 11) ORDER BY brand_prefix;
