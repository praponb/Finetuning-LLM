SELECT
    product_id,
    COUNT(*) AS store_sale_count,
    SUM(revenue) AS total_revenue,
    MIN(revenue) AS min_revenue
FROM store_sales
WHERE YEAR(sale_date) >= 2025 AND revenue IS NOT NULL
GROUP BY product_id
HAVING total_revenue >= 15000;
