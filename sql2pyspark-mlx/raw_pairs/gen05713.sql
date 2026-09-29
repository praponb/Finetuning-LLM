SELECT sale_cohort, avg_revenue, avg_units
FROM (
    SELECT CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END sale_cohort, AVG(revenue) avg_revenue, AVG(units) avg_units
    FROM store_sales
    GROUP BY CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END
) t
WHERE avg_revenue < 3000
