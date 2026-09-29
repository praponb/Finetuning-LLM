SELECT i.id, i.store_id, i.product_id, i.quantity, ss.sale_date FROM inventory i JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id;
