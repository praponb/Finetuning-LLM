SELECT
    category,
    CASE WHEN cost >= 660 THEN 'high' WHEN cost >= 120 THEN 'mid' ELSE 'low' END AS cost_bucket,
    brand,
    MIN(launch_date) AS first_launch_date,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list,
    COUNT(*) AS product_count
FROM products
WHERE (launch_date BETWEEN '2024-06-24' AND '2024-09-30' AND product_name LIKE 'D%') OR SIZE(tags) >= 1
GROUP BY category, CASE WHEN cost >= 660 THEN 'high' WHEN cost >= 120 THEN 'mid' ELSE 'low' END, brand
ORDER BY first_launch_date;
