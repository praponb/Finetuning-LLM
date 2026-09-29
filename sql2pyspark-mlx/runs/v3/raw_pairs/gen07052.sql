SELECT i.last_restock, i.product_id, ss.product_id AS store_sale_product_id FROM inventory i JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id ORDER BY product_id DESC;
