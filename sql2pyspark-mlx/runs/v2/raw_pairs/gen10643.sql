SELECT
    MONTH(sale_date) AS sale_month,
    YEAR(sale_date) AS sale_year,
    MAX(units) - MIN(units) AS units_range,
    COUNT(DISTINCT revenue) AS unique_stores
FROM store_sales
GROUP BY MONTH(sale_date), YEAR(sale_date)
HAVING MAX(units) - MIN(units) >= 24
ORDER BY sale_month DESC
