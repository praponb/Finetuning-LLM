SELECT
    store_id,
    CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort,
    COUNT(DISTINCT revenue) AS unique_products,
    ROUND(STDDEV(units), 2) AS stddev_units
FROM store_sales
WHERE revenue IS NULL AND sale_date < '2023-11-27' AND units <> 25
GROUP BY 1, 2
HAVING COUNT(revenue) >= 5;
