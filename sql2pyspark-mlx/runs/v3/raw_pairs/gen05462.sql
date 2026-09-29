SELECT
    id,
    dept_id,
    salary,
    hire_date,
    FIRST_VALUE(hire_date) OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS first_hire_date
FROM employees
WHERE manager_id = 16
ORDER BY dept_id DESC
