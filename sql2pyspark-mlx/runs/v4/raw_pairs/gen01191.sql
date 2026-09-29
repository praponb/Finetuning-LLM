SELECT
    id,
    manager_id,
    dept_id,
    salary,
    COUNT(*) OVER (PARTITION BY manager_id) AS manager_id_count,
    AVG(salary) OVER (PARTITION BY dept_id) AS dept_id_avg_salary
FROM employees
