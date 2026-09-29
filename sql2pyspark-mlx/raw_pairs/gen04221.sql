SELECT
    id,
    email,
    dept_id,
    hire_date,
    is_remote,
    level,
    LAG(salary) OVER (PARTITION BY dept_id, manager_id ORDER BY salary, id) AS prev_salary,
    ROW_NUMBER() OVER (PARTITION BY dept_id, manager_id ORDER BY salary, id) AS seq_no
FROM employees
