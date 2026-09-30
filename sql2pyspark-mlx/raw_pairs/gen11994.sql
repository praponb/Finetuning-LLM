SELECT ss.units, p.launch_date, p.stock_qty, s.city FROM store_sales ss INNER JOIN products p ON p.id = ss.product_id INNER JOIN suppliers s ON p.supplier_id = s.id ORDER BY s.city DESC, 2 DESC;
