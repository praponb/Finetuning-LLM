SELECT
    products.id,
    products.category,
    COUNT(store_sales.id) store_sale_count,
    MIN(store_sales.revenue) min_revenue
FROM products
LEFT JOIN store_sales ON store_sales.product_id = products.id
WHERE store_sales.sale_date > '2024-06-01'
GROUP BY products.id, products.category
ORDER BY id DESC
