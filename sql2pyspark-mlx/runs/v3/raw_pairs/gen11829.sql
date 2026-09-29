SELECT
    id,
    manager_id,
    salary,
    AVG(salary) OVER (PARTITION BY manager_id) manager_id_avg_salary
FROM employees;
