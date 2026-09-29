SELECT ss.id, ss.revenue, ss.product_id, p.launch_date FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id
