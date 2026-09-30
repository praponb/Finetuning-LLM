SELECT i.id, i.last_restock, i.store_id, ss.revenue FROM inventory i INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id WHERE ss.revenue < 2800 ORDER BY store_id;
