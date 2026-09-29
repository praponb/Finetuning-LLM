SELECT
    id,
    dept_id,
    hire_date,
    salary,
    level,
    LEAD(salary, 1, 0) OVER (PARTITION BY dept_id ORDER BY hire_date, id) AS next_salary,
    SUM(salary) OVER (PARTITION BY level ORDER BY hire_date, id) AS running_salary
FROM employees
WHERE job_title IN ('Intern', 'Engineer')
ORDER BY level, 2;
