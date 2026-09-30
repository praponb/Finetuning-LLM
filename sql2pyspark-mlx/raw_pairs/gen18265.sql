SELECT
    DATE_TRUNC('MONTH', sale_date) AS sale_month,
    MAX(revenue) - MIN(revenue) AS revenue_range,
    COUNT(DISTINCT units) AS unique_products,
    ROUND(AVG(units), 2) AS avg_units
FROM store_sales
WHERE revenue > 3800 AND sale_date <= '2024-09-01' AND sale_date <= '2023-10-27'
GROUP BY 1
HAVING ROUND(AVG(units), 2) < 48
ORDER BY sale_month DESC
