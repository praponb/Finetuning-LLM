SELECT
    store_sales.product_id,
    store_sales.store_id,
    products.launch_date,
    products.cost
FROM store_sales
INNER JOIN products ON store_sales.product_id = products.id
