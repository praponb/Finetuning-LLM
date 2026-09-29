SELECT
    d.dept_name,
    d.location,
    t.intern_salary,
    t.job_title_values
FROM departments d
INNER JOIN (SELECT dept_id, SUM(CASE WHEN job_title = 'Intern' THEN salary ELSE 0 END) intern_salary, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) job_title_values
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
ORDER BY 4 ASC
