SELECT
    d.location,
    d.budget,
    t.designer_count,
    t.min_hire_date
FROM departments d
LEFT JOIN (SELECT dept_id, SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END) designer_count, MIN(hire_date) min_hire_date
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
ORDER BY location;
