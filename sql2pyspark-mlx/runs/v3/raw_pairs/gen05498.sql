SELECT
    location,
    dept_name,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
    AVG(budget) AS avg_budget
FROM departments
WHERE budget > 1758000
GROUP BY location, dept_name
