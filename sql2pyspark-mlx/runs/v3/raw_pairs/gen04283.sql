SELECT
    store_sales.id,
    store_sales.store_id,
    store_sales.revenue,
    stores.opened_date,
    products.cost
FROM store_sales
FULL OUTER JOIN stores ON store_sales.store_id = stores.id
INNER JOIN products ON store_sales.product_id = products.id
WHERE store_sales.store_id = 1;
