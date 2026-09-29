SELECT
    store_sales.id,
    store_sales.product_id,
    store_sales.store_id,
    stores.store_name,
    products.price
FROM store_sales
JOIN stores ON stores.id = store_sales.store_id
LEFT JOIN products ON store_sales.product_id = products.id
WHERE stores.city NOT IN ('Berlin', 'New York', 'London') AND store_sales.revenue <= 1300;
