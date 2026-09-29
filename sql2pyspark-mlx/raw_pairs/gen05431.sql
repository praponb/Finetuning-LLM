SELECT
    id,
    level,
    hire_date,
    ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date, id) AS row_num
FROM employees
WHERE manager_id IN (22, 33);
