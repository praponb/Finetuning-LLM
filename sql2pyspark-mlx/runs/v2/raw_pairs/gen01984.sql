SELECT id, brand, supplier_id, launch_date, ROW_NUMBER() OVER (PARTITION BY brand, supplier_id ORDER BY launch_date, id) AS launch_date_row FROM products;
