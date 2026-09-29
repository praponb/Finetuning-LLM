SELECT tags, brand, product_name FROM products WHERE stock_qty > 381 AND MONTH(launch_date) IN (1, 3, 12) ORDER BY brand
