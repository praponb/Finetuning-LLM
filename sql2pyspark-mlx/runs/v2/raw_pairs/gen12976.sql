WITH department_summary AS (
    SELECT dept_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values
    FROM employees
    GROUP BY dept_id
)
SELECT d.location, ds.job_title_values
FROM departments d
INNER JOIN department_summary ds ON d.id = ds.dept_id
ORDER BY d.location;
