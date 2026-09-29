SELECT ss.revenue, ss.store_id, p.launch_date, p.brand FROM store_sales ss JOIN products p ON ss.product_id = p.id WHERE LOWER(p.product_name) LIKE '%esk%';
