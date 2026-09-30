SELECT ss.units, ss.product_id, p.launch_date, s.supplier_name FROM store_sales ss JOIN products p ON ss.product_id = p.id LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
