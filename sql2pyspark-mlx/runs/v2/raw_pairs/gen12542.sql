SELECT supplier_id, brand, UPPER(brand) brand_upper FROM products WHERE price IS NULL AND (launch_date >= '2023-09-01' OR stock_qty >= 291);
