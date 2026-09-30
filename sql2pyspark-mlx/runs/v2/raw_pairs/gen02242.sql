SELECT
    DATE_FORMAT(sale_date, 'yyyy-MM') sale_month,
    store_id,
    MAX(revenue) - MIN(revenue) revenue_range,
    COUNT(revenue) revenue_count
FROM store_sales
WHERE product_id IN (5, 6, 9) AND YEAR(sale_date) = 2024
GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM'), store_id
HAVING AVG(revenue) >= 1700
ORDER BY store_id DESC;
