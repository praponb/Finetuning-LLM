SELECT ss.units, ss.store_id, p.launch_date, p.stock_qty, s.city, s.store_name FROM store_sales ss LEFT JOIN products p ON ss.product_id = p.id LEFT JOIN stores s ON s.id = ss.store_id;
