WITH department_totals AS (
    SELECT dept_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values
    FROM employees
    WHERE job_title = 'Engineer'
    GROUP BY dept_id
)
SELECT d.id, d.budget, dt.job_title_values
FROM departments d
INNER JOIN department_totals dt ON d.id = dt.dept_id
ORDER BY job_title_values, id DESC
