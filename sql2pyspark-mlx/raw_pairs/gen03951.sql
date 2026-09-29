SELECT launch_cohort, category_values, min_stock_qty
FROM (
    SELECT CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values, MIN(stock_qty) AS min_stock_qty
    FROM products
    GROUP BY CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END
) AS s
WHERE min_stock_qty < 310
ORDER BY category_values, launch_cohort
LIMIT 5
