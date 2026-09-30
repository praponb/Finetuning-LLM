SELECT CASE WHEN budget >= 3222000 THEN 'high' WHEN budget >= 1801000 THEN 'mid' ELSE 'low' END AS budget_bucket, dept_name, SORT_ARRAY(COLLECT_SET(location)) AS location_list
FROM departments
WHERE budget >= 1807000 AND dept_name <> 'Marketing'
GROUP BY CASE WHEN budget >= 3222000 THEN 'high' WHEN budget >= 1801000 THEN 'mid' ELSE 'low' END, dept_name
ORDER BY dept_name ASC
