SELECT YEAR(launch_date) AS launch_year, SORT_ARRAY(COLLECT_SET(category)) AS category_list FROM products GROUP BY YEAR(launch_date) ORDER BY launch_year DESC
