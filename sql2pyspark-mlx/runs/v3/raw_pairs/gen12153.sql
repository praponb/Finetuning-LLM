SELECT price, brand, supplier_id, QUARTER(launch_date) AS launch_date_quarter FROM products WHERE product_name LIKE '%Runn%'
