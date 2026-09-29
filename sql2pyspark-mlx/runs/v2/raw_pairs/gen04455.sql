SELECT
    launch_month,
    launch_year,
    umbrella_count,
    category_list
FROM (
    SELECT
        DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month,
        YEAR(launch_date) AS launch_year,
        COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) AS umbrella_count,
        SORT_ARRAY(COLLECT_SET(category)) AS category_list
    FROM products
    GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM'), YEAR(launch_date)
) AS x
WHERE umbrella_count > 5
ORDER BY launch_month, launch_year
LIMIT 10;
