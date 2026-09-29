SELECT i.id, i.store_id, ss.units, ss.product_id, ss.sale_date FROM inventory i LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id WHERE i.quantity = 146
