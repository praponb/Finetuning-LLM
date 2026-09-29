SELECT store_sales.id, store_sales.store_id, store_sales.units, stores.opened_date FROM store_sales RIGHT JOIN stores ON store_sales.store_id = stores.id WHERE UPPER(stores.store_name) LIKE '%IRP%';
