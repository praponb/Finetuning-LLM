SELECT ROUND(STDDEV(budget), 2) stddev_budget, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) location_values
FROM departments
WHERE dept_name IN ('Engineering', 'HR', 'Sales') AND (dept_name IN ('Engineering', 'HR') OR budget > 3734000)
ORDER BY stddev_budget DESC
