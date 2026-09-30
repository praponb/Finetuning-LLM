SELECT DATE_TRUNC('MONTH', launch_date) AS launch_month, COUNT(*) AS total_count, SORT_ARRAY(COLLECT_SET(brand)) AS brand_list
FROM products
WHERE product_name IS NOT NULL
GROUP BY DATE_TRUNC('MONTH', launch_date)
HAVING COUNT(*) > 1
