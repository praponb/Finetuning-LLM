SELECT
    d.id,
    d.dept_name,
    d.budget,
    t.job_title_list,
    t.min_hire_date
FROM departments d
INNER JOIN (SELECT dept_id, SORT_ARRAY(COLLECT_SET(job_title)) job_title_list, MIN(hire_date) min_hire_date
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
ORDER BY min_hire_date;
