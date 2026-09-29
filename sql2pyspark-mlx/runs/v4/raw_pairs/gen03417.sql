SELECT
    products.supplier_id,
    products.brand,
    SUM(store_sales.revenue) AS total_revenue,
    COUNT(DISTINCT store_sales.units) AS unique_products
FROM store_sales
LEFT OUTER JOIN stores ON stores.id = store_sales.store_id
JOIN products ON store_sales.product_id = products.id
GROUP BY products.supplier_id, products.brand;
