SELECT
    store_sales.id,
    store_sales.revenue,
    store_sales.product_id,
    stores.region
FROM store_sales
INNER JOIN stores ON stores.id = store_sales.store_id
WHERE store_sales.units BETWEEN 14 AND 32 AND store_sales.product_id IN (1, 3, 5, 9)
ORDER BY stores.region DESC, id
LIMIT 50
