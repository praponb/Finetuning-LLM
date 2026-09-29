SELECT COUNT(budget) AS budget_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values
FROM departments
WHERE dept_name NOT IN ('Engineering', 'Finance') AND dept_name = 'Finance';
