SELECT
    store_sales.revenue,
    stores.opened_date,
    stores.region,
    products.supplier_id
FROM store_sales
INNER JOIN stores ON stores.id = store_sales.store_id
INNER JOIN products ON store_sales.product_id = products.id
ORDER BY revenue
