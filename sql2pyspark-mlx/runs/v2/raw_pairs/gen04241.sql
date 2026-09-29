WITH employees_by_department AS (
    SELECT dept_id, SUM(salary) AS total_salary, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values
    FROM employees
    WHERE full_name IS NOT NULL
    GROUP BY dept_id
)
SELECT
    d.id,
    d.budget,
    d.dept_name,
    ebd.total_salary,
    ebd.level_values
FROM departments d
LEFT OUTER JOIN employees_by_department ebd ON d.id = ebd.dept_id
ORDER BY d.dept_name
