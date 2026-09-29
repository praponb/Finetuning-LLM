SELECT
    stores.id,
    stores.store_name,
    COUNT(store_sales.id) AS store_sale_count,
    MIN(store_sales.sale_date) AS first_sale_date
FROM stores
LEFT JOIN store_sales ON stores.id = store_sales.store_id
GROUP BY stores.id, stores.store_name
ORDER BY stores.store_name;
