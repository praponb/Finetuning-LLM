SELECT
    store_id,
    CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort,
    product_id,
    MIN(sale_date) AS min_sale_date
FROM store_sales
GROUP BY 1, 2, 3
ORDER BY store_id DESC, product_id DESC;
