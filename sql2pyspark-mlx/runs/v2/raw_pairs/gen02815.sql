SELECT ss.id, ss.store_id, ss.units, ss.product_id, p.launch_date, p.category FROM store_sales ss JOIN products p ON ss.product_id = p.id
