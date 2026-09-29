SELECT i.product_id, i.store_id, i.last_restock, ss.units, ss.revenue FROM inventory i LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id ORDER BY 1, revenue DESC;
