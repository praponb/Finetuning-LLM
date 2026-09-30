SELECT
    store_sales.id,
    store_sales.store_id,
    store_sales.units,
    store_sales.product_id,
    products.price,
    products.launch_date,
    products.brand
FROM store_sales
RIGHT JOIN products ON store_sales.product_id = products.id
WHERE store_sales.product_id IN (2, 4, 6, 10);
