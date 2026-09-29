SELECT
    id,
    level,
    hire_date,
    RANK() OVER (PARTITION BY level ORDER BY hire_date DESC) AS rnk
FROM employees;
