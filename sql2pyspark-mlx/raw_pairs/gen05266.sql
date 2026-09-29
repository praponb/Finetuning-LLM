WITH department_summary AS (
    SELECT dept_id, MIN(salary) min_salary, SORT_ARRAY(COLLECT_SET(level)) level_list
    FROM employees
    WHERE is_remote = TRUE
    GROUP BY dept_id
)
SELECT
    d.id,
    d.budget,
    ds.min_salary,
    ds.level_list
FROM departments d
INNER JOIN department_summary ds ON d.id = ds.dept_id
ORDER BY 3 DESC;
