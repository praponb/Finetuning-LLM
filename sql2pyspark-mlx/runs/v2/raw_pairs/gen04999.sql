SELECT
    dept_name,
    ROUND((100 * SUM(CASE WHEN location = 'New York' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_new_york,
    SUM(CASE WHEN dept_name = 'Marketing' THEN 1 ELSE 0 END) AS marketing_count,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM departments
GROUP BY dept_name
