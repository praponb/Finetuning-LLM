SELECT ss.id, ss.product_id, ss.store_id, ss.revenue, p.cost, p.launch_date, p.brand FROM store_sales ss JOIN products p ON ss.product_id = p.id;
