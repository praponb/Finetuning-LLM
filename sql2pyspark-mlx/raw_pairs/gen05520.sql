WITH department_totals AS (
    SELECT dept_id, SUM(salary) AS total_salary, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values
    FROM employees
    WHERE level = 'L3'
    GROUP BY dept_id
)
SELECT
    departments.dept_name,
    departments.budget,
    department_totals.total_salary,
    department_totals.job_title_values
FROM departments
LEFT JOIN department_totals ON departments.id = department_totals.dept_id
WHERE department_totals.total_salary > 660000
