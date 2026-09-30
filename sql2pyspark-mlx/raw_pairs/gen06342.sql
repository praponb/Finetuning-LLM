SELECT
    id,
    email,
    dept_id,
    salary,
    hire_date,
    is_remote
FROM employees
QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary ASC, id ASC) = 1
