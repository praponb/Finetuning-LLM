WITH department_summary AS (
    SELECT dept_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values, MIN(hire_date) AS first_hire_date
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    ds.level_values,
    ds.first_hire_date
FROM departments d
LEFT JOIN department_summary ds ON d.id = ds.dept_id
ORDER BY id DESC;
