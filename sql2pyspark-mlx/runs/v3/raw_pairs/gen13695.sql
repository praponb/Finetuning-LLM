SELECT sale_month, revenue_count, unique_stores
FROM (
    SELECT MONTH(sale_date) AS sale_month, COUNT(revenue) AS revenue_count, COUNT(DISTINCT units) AS unique_stores
    FROM store_sales
    WHERE YEAR(sale_date) >= 2023
    GROUP BY MONTH(sale_date)
) AS agg
WHERE revenue_count > 4
ORDER BY sale_month DESC
