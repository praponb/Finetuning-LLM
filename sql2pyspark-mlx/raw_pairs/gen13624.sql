SELECT id, launch_date, supplier_id FROM products WHERE launch_date = (SELECT MIN(launch_date) FROM products) ORDER BY supplier_id;
