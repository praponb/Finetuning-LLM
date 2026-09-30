SELECT
    stores.city,
    stores.region,
    stores.sqft,
    store_sales.store_id,
    store_sales.units,
    store_sales.product_id
FROM stores
LEFT OUTER JOIN store_sales ON stores.id = store_sales.store_id
WHERE store_sales.product_id IN (5, 9)
ORDER BY store_sales.store_id DESC
