SELECT
    product_id,
    CASE WHEN units >= 42 THEN 'high' WHEN units >= 15 THEN 'mid' ELSE 'low' END AS units_bucket,
    CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort,
    COUNT(revenue) AS revenue_count,
    COUNT(DISTINCT revenue) AS unique_stores
FROM store_sales
GROUP BY product_id, units_bucket, sale_cohort
HAVING COUNT(DISTINCT revenue) < 1
ORDER BY unique_stores;
