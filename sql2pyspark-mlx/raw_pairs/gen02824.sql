SELECT i.id, i.last_restock, ss.units, ss.sale_date FROM inventory i INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id WHERE ss.revenue IS NOT NULL
