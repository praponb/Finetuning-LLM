SELECT
    CASE WHEN units >= 41 THEN 'high' WHEN units >= 19 THEN 'mid' ELSE 'low' END units_bucket,
    QUARTER(sale_date) sale_quarter,
    AVG(units) avg_units,
    MAX(revenue) - MIN(revenue) revenue_range
FROM store_sales
GROUP BY 1, 2
ORDER BY sale_quarter;
