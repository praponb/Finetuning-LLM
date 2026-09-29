SELECT launch_date, stock_qty, brand, UPPER(TRIM(product_name)) AS product_name_normalized FROM products;
