SELECT id, brand, stock_qty, LENGTH(brand) AS brand_length, ADD_MONTHS(launch_date, 6) AS launch_date_plus_months FROM products WHERE cost > 260 AND cost > 220
