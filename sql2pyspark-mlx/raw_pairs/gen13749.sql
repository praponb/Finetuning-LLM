SELECT i.id, i.store_id, ss.product_id, ss.sale_date FROM inventory i INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id WHERE i.quantity <= 105
