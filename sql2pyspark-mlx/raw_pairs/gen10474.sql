SELECT i.quantity, i.store_id, ss.store_id store_sale_store_id, ss.product_id, ss.units FROM inventory i INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id;
