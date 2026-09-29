SELECT store_sales.id, store_sales.product_id, stores.city
FROM store_sales
INNER JOIN stores ON store_sales.store_id = stores.id
