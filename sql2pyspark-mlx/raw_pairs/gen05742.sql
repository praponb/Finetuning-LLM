WITH employees_by_department AS (
    SELECT dept_id, SUM(salary) total_salary, SUM(CASE WHEN job_title = 'Intern' THEN salary ELSE 0 END) intern_salary
    FROM employees
    WHERE is_remote = FALSE
    GROUP BY dept_id
)
SELECT d.dept_name, ebd.total_salary, ebd.intern_salary
FROM departments d
INNER JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.intern_salary > 76;
