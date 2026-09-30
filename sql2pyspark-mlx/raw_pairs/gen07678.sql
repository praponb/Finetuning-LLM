SELECT
    id,
    manager_id,
    salary,
    level,
    MAX(salary) OVER (PARTITION BY manager_id) manager_id_max_salary,
    MAX(salary) OVER (PARTITION BY level) level_max_salary
FROM employees;
