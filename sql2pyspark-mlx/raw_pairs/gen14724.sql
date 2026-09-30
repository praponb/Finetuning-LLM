SELECT
    id,
    manager_id,
    salary,
    ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary DESC, id) AS row_num
FROM employees
WHERE manager_id = 18
