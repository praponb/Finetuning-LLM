SELECT
    products.price,
    products.brand,
    store_sales.product_id,
    LEAST(store_sales.revenue, store_sales.units) AS least_revenue_units
FROM products
INNER JOIN store_sales ON products.id = store_sales.product_id
WHERE ARRAY_CONTAINS(products.tags, 'new');
