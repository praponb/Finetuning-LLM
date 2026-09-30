SELECT *
FROM (
    SELECT dept_name, SORT_ARRAY(COLLECT_SET(location)) AS location_list, ROUND(AVG(budget), 2) AS avg_budget
    FROM departments
    WHERE location NOT IN ('Berlin', 'Bangkok', 'London')
    GROUP BY dept_name
) AS s
WHERE avg_budget > 71
ORDER BY avg_budget DESC;
