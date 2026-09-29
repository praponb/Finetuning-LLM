SELECT store_sales.units, store_sales.product_id, store_sales.revenue, stores.opened_date FROM store_sales JOIN stores ON stores.id = store_sales.store_id
