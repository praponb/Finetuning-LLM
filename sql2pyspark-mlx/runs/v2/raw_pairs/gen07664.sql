SELECT ss.id, ss.product_id, ss.sale_date, ss.units FROM store_sales ss WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE LENGTH(store_name) > 7);
