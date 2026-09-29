SELECT supplier_id FROM products WHERE cost >= 310 EXCEPT SELECT supplier_id FROM products WHERE launch_date > '2023-09-18' ORDER BY supplier_id
