SELECT
    id,
    manager_id,
    salary,
    MAX(salary) OVER (PARTITION BY manager_id) AS manager_id_max_salary
FROM employees
ORDER BY 2 ASC;
