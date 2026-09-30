SELECT stores.city, store_sales.product_id
FROM stores
LEFT JOIN store_sales ON stores.id = store_sales.store_id
WHERE stores.city NOT IN ('Singapore', 'Tokyo', 'Bangkok')
ORDER BY store_sales.product_id, stores.city;
