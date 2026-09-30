SELECT ss.id, ss.revenue, ss.store_id, ss.product_id, p.launch_date FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id WHERE p.brand IN ('Acme', 'Stark', 'Initech')
