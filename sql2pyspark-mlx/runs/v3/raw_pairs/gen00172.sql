SELECT
    YEAR(sale_date) sale_year,
    CASE WHEN units >= 39 THEN 'high' WHEN units >= 7 THEN 'mid' ELSE 'low' END units_bucket,
    MAX(sale_date) latest_sale_date,
    COUNT(revenue) revenue_count,
    COUNT(*) total_count
FROM store_sales
GROUP BY sale_year, units_bucket
HAVING COUNT(*) >= 3
ORDER BY total_count;
