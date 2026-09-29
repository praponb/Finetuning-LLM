SELECT ss.revenue, s.store_name, p.cost, p.launch_date FROM store_sales ss JOIN stores s ON s.id = ss.store_id JOIN products p ON p.id = ss.product_id
