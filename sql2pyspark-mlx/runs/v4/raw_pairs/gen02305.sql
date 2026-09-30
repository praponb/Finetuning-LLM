SELECT
    dept_name,
    AVG(budget) AS avg_budget,
    MAX(budget) - MIN(budget) AS budget_range,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list
FROM departments
GROUP BY dept_name
ORDER BY avg_budget
