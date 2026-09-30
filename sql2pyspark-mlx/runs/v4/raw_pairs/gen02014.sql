SELECT d.location, t.unique_levels, t.job_title_list
FROM departments d
LEFT JOIN (SELECT dept_id, COUNT(DISTINCT salary) unique_levels, SORT_ARRAY(COLLECT_SET(job_title)) job_title_list
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
ORDER BY location DESC;
