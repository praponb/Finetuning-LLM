SELECT
    store_sales.id,
    store_sales.revenue,
    store_sales.store_id,
    stores.sqft,
    stores.opened_date,
    products.launch_date
FROM store_sales
FULL OUTER JOIN stores ON store_sales.store_id = stores.id
FULL OUTER JOIN products ON store_sales.product_id = products.id
WHERE LENGTH(products.product_name) > 12 AND store_sales.sale_date >= '2024-01-09';
