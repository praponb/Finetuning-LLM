SELECT
    category,
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    SIZE(COLLECT_SET(category)) AS num_categories
FROM products
GROUP BY 1, 2, 3
ORDER BY category;
