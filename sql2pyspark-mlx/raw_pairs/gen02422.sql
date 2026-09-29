SELECT YEAR(launch_date) AS launch_year, COUNT(*) AS row_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values FROM products GROUP BY YEAR(launch_date) ORDER BY row_count DESC;
