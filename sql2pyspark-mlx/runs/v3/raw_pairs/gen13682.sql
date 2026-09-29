SELECT
    d.id,
    d.dept_name,
    COUNT(e.id) AS employee_count,
    SUM(CASE WHEN e.job_title = 'Intern' THEN e.salary ELSE 0 END) AS intern_salary,
    SORT_ARRAY(COLLECT_SET(e.level)) AS level_list
FROM departments d
LEFT OUTER JOIN employees e ON d.id = e.dept_id
GROUP BY d.id, d.dept_name;
