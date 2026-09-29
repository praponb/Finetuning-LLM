SELECT
    departments.location,
    departments.dept_name,
    t.total_salary,
    t.director_salary
FROM departments
LEFT OUTER JOIN (SELECT dept_id, SUM(salary) AS total_salary, SUM(CASE WHEN job_title = 'Director' THEN salary ELSE 0 END) AS director_salary
    FROM employees
    GROUP BY dept_id) t ON departments.id = t.dept_id
WHERE t.director_salary >= 30
ORDER BY departments.location ASC, dept_name ASC;
