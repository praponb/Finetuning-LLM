SELECT id, supplier_id, launch_date, ADD_MONTHS(launch_date, 12) AS launch_date_plus_months, CONCAT_WS('-', category, product_name) AS category_product_name_key FROM products
