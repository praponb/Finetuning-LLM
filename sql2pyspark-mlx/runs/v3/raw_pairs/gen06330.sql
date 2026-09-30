SELECT
    id,
    level,
    hire_date,
    ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date, id) AS hire_date_row
FROM employees
WHERE full_name LIKE 'Hen%';
