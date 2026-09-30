SELECT
    id,
    manager_id,
    salary,
    ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary DESC, id) AS row_num,
    SUM(salary) OVER (PARTITION BY manager_id) AS manager_id_sum_salary
FROM employees
ORDER BY salary, manager_id
