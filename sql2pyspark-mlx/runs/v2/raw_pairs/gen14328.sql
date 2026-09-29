WITH department_stats AS (
    SELECT dept_id, SORT_ARRAY(COLLECT_SET(level)) AS level_list, SUM(salary) AS total_salary
    FROM employees
    GROUP BY dept_id
)
SELECT d.dept_name, ds.level_list, ds.total_salary
FROM departments d
INNER JOIN department_stats ds ON d.id = ds.dept_id;
