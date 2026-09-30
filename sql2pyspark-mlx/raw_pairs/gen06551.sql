SELECT
    store_sales.id,
    store_sales.store_id,
    store_sales.revenue,
    stores.store_name,
    stores.city,
    products.cost
FROM store_sales
JOIN stores ON store_sales.store_id = stores.id
LEFT JOIN products ON store_sales.product_id = products.id
ORDER BY store_sales.store_id ASC, city ASC
