SELECT
    store_id,
    YEAR(sale_date) AS sale_year,
    MAX(revenue) AS max_revenue,
    COUNT(units) AS units_count
FROM store_sales
WHERE units <= 16 AND sale_date <= '2025-10-17'
GROUP BY 1, 2
ORDER BY sale_year
