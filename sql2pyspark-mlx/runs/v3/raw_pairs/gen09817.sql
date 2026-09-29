SELECT
    product_id,
    DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month,
    CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort,
    SUM(revenue) AS total_revenue
FROM store_sales
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 3
ORDER BY 4 DESC, sale_month;
