SELECT
    store_sales.id,
    store_sales.store_id,
    stores.region,
    products.category,
    DATE_ADD(products.launch_date, 64) AS launch_date_plus_64d
FROM store_sales
JOIN stores ON store_sales.store_id = stores.id
FULL OUTER JOIN products ON store_sales.product_id = products.id
ORDER BY 4;
