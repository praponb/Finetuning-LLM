SELECT
    store_id,
    product_id,
    MONTH(sale_date) AS sale_month,
    COUNT(DISTINCT revenue) AS unique_products,
    COUNT(*) AS total_count
FROM store_sales
WHERE sale_date <= '2025-07-01'
GROUP BY store_id, product_id, MONTH(sale_date)
ORDER BY unique_products;
