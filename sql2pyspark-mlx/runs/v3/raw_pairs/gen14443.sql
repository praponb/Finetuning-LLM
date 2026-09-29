SELECT
    stores.region,
    stores.opened_date,
    stores.city,
    store_sales.sale_date,
    store_sales.revenue,
    store_sales.store_id,
    SUBSTRING(stores.store_name, 1, 3) AS store_name_prefix
FROM stores
LEFT JOIN store_sales ON store_sales.store_id = stores.id
ORDER BY store_name_prefix ASC
