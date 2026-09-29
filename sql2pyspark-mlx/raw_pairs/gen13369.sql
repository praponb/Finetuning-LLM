WITH employees_by_department AS (
    SELECT dept_id, MIN(salary) min_salary, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) job_title_values
    FROM employees
    WHERE hire_date > '2025-09-05'
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    d.location,
    ebd.min_salary,
    ebd.job_title_values
FROM departments d
LEFT OUTER JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.min_salary > 134000
ORDER BY dept_name DESC
