SELECT
    ss.store_id,
    CASE WHEN YEAR(ss.sale_date) < 2024 THEN 'before_2024' WHEN YEAR(ss.sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort,
    COUNT(i.quantity) AS quantity_count,
    COUNT(DISTINCT i.quantity) AS unique_stores
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.store_id, CASE WHEN YEAR(ss.sale_date) < 2024 THEN 'before_2024' WHEN YEAR(ss.sale_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY store_id DESC
