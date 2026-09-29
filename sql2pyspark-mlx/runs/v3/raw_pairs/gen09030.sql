SELECT
    products.brand,
    products.launch_date,
    store_sales.product_id,
    store_sales.store_id,
    store_sales.revenue - store_sales.units AS revenue_per_unit
FROM products
LEFT OUTER JOIN store_sales ON products.id = store_sales.product_id
WHERE products.cost IS NOT NULL
