SELECT
    id,
    level,
    hire_date,
    salary,
    dept_id,
    LAG(salary, 1) OVER (PARTITION BY level ORDER BY hire_date, id) AS prev_salary,
    ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) AS hire_date_row
FROM employees
WHERE level IN ('L1', 'L5')
