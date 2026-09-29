SELECT
    store_id,
    DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month,
    MAX(revenue) - MIN(revenue) AS revenue_range,
    MIN(revenue) AS min_revenue
FROM store_sales
WHERE revenue IS NOT NULL
GROUP BY store_id, DATE_FORMAT(sale_date, 'yyyy-MM')
HAVING MAX(units) < 21
ORDER BY min_revenue;
