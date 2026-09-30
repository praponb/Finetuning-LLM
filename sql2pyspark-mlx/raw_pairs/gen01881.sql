WITH employees_by_department AS (
    SELECT dept_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) job_title_values
    FROM employees
    GROUP BY dept_id
)
SELECT d.id, d.budget, ebd.job_title_values
FROM departments d
INNER JOIN employees_by_department ebd ON d.id = ebd.dept_id;
