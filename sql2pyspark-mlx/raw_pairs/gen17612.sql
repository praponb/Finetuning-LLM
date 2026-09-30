SELECT
    DATE_TRUNC('MONTH', sale_date) AS sale_month,
    QUARTER(sale_date) AS sale_quarter,
    COUNT(units) AS units_count,
    MIN(sale_date) AS min_sale_date,
    COUNT(revenue) AS revenue_count
FROM store_sales
GROUP BY sale_month, sale_quarter
HAVING COUNT(revenue) >= 1
ORDER BY revenue_count DESC
