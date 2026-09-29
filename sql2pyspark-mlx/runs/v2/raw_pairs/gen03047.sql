SELECT
    id,
    dept_id,
    salary,
    level,
    hire_date,
    MAX(salary) OVER (PARTITION BY dept_id) AS dept_id_max_salary,
    LEAD(salary, 1) OVER (PARTITION BY level ORDER BY hire_date, id) AS next_salary
FROM employees
WHERE full_name LIKE 'Em%'
ORDER BY level;
