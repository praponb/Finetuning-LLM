SELECT i.product_id, ss.revenue, ss.sale_date, ss.units FROM inventory i INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id ORDER BY ss.sale_date;
