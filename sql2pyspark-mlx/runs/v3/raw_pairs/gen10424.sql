SELECT i.store_id, i.quantity, ss.store_id store_sale_store_id FROM inventory i JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
