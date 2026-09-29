SELECT
    YEAR(sale_date) AS sale_year,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS cnt,
    ROUND(SUM(revenue) * 100.0 / SUM(SUM(revenue)) OVER (), 2) AS pct_of_total
FROM store_sales
WHERE revenue IS NOT NULL
GROUP BY YEAR(sale_date)
ORDER BY pct_of_total ASC
