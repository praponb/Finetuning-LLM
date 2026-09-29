SELECT
    d.id,
    d.dept_name,
    t.max_salary,
    t.last_hire_date
FROM departments d
JOIN (SELECT dept_id, MAX(salary) AS max_salary, MAX(hire_date) AS last_hire_date
    FROM employees
    WHERE job_title IN ('Intern', 'Director')
    GROUP BY dept_id) t ON d.id = t.dept_id
