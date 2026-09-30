SELECT
    id,
    dept_id,
    salary,
    MAX(salary) OVER (PARTITION BY dept_id) AS dept_id_max_salary
FROM employees
WHERE hire_date BETWEEN '2025-01-24' AND '2025-06-01'
ORDER BY salary DESC;
