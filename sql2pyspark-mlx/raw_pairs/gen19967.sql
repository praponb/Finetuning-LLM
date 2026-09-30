SELECT
    id,
    manager_id,
    hire_date,
    salary,
    LEAD(salary, 2) OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS next_salary
FROM employees;
