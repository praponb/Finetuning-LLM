SELECT
    id,
    manager_id,
    hire_date,
    dept_id,
    ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS rn,
    NTILE(4) OVER (PARTITION BY manager_id, dept_id ORDER BY hire_date, id) AS bucket
FROM employees
ORDER BY rn DESC;
