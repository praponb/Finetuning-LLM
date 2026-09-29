SELECT
    CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort,
    SUM(units) AS total_units,
    COUNT(*) AS cnt,
    SUM(SUM(units)) OVER (ORDER BY CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END) AS cumulative_units
FROM store_sales
GROUP BY CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY sale_cohort DESC, cnt DESC;
