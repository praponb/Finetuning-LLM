SELECT stores.id, stores.store_name, stores.region, stores.opened_date FROM stores WHERE EXISTS (SELECT 1 FROM store_sales WHERE store_sales.store_id = stores.id) ORDER BY stores.region DESC
