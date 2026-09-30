SELECT dept_name, MAX(budget) - MIN(budget) AS budget_range, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM departments
WHERE NOT (dept_name IN ('Engineering', 'HR') AND location IN ('Singapore', 'Chiang Mai'))
GROUP BY dept_name
HAVING MAX(budget) - MIN(budget) >= 31
ORDER BY 1 ASC
LIMIT 10;
