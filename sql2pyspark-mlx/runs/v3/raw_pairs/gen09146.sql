SELECT
    YEAR(sale_date) AS sale_year,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS cnt,
    SUM(SUM(revenue)) OVER (ORDER BY YEAR(sale_date)) AS cumulative_revenue
FROM store_sales
WHERE sale_date BETWEEN '2023-12-01' AND '2025-08-01'
GROUP BY YEAR(sale_date)
ORDER BY sale_year DESC;
