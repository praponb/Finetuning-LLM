SELECT
    store_sales.id,
    store_sales.units,
    store_sales.sale_date,
    products.brand,
    stores.sqft,
    stores.city
FROM store_sales
LEFT JOIN products ON store_sales.product_id = products.id
INNER JOIN stores ON stores.id = store_sales.store_id
WHERE products.product_name IS NOT NULL AND store_sales.units >= 12
ORDER BY brand DESC, 5 DESC
