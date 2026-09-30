SELECT
    id,
    dept_id,
    hire_date,
    salary,
    LEAD(salary, 2) OVER (PARTITION BY dept_id ORDER BY hire_date, id) next_salary
FROM employees
WHERE salary < 56000
