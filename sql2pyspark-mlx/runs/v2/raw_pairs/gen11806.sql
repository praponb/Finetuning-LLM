SELECT
    store_sales.id,
    store_sales.sale_date,
    store_sales.product_id,
    stores.city,
    products.cost,
    products.product_name,
    ROUND(store_sales.revenue, 2) revenue_rounded
FROM store_sales
JOIN stores ON store_sales.store_id = stores.id
LEFT JOIN products ON store_sales.product_id = products.id
WHERE UPPER(stores.store_name) LIKE '%OWN%'
ORDER BY stores.city DESC;
