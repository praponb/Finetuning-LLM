SELECT store_sales.store_id, stores.opened_date, stores.city, stores.region FROM store_sales RIGHT OUTER JOIN stores ON stores.id = store_sales.store_id ORDER BY stores.opened_date
