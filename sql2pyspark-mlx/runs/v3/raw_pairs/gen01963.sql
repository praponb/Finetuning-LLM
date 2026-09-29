SELECT QUARTER(launch_date) AS launch_quarter, SORT_ARRAY(COLLECT_SET(category)) AS category_list, ROUND(AVG(price), 2) AS avg_price
FROM products
WHERE supplier_id IS NULL
GROUP BY QUARTER(launch_date)
ORDER BY launch_quarter DESC
LIMIT 3;
