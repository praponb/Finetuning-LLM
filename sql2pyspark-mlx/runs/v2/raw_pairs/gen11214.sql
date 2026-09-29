SELECT
    id,
    manager_id,
    level,
    salary,
    ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY salary, id) AS salary_row
FROM employees
WHERE salary >= 96000;
