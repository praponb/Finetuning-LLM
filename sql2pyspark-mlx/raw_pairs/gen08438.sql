SELECT dept_name, COUNT(budget) AS budget_count, SORT_ARRAY(COLLECT_SET(location)) AS location_list
FROM departments
WHERE location IN ('Singapore', 'Bangkok', 'Berlin')
GROUP BY 1
ORDER BY dept_name
