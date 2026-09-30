SELECT ss.id, ss.sale_date, p.category, p.supplier_id, p.launch_date FROM store_sales ss LEFT JOIN products p ON ss.product_id = p.id WHERE ss.units = 15 AND ss.sale_date > '2023-11-01'
