SELECT location, SUM(CASE WHEN location = 'Berlin' THEN budget ELSE 0 END) AS berlin_budget, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM departments
WHERE budget >= 2610000 AND budget < 3243000
GROUP BY location
ORDER BY location, berlin_budget DESC
