SELECT dept_name, COUNT(*) AS row_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values
FROM departments
GROUP BY dept_name
ORDER BY location_values ASC, dept_name ASC
LIMIT 20;
