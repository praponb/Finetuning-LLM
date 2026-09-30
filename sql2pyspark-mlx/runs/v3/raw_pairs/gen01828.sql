SELECT *
FROM (
    SELECT
        id,
        level,
        hire_date,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY hire_date DESC) AS rn
    FROM employees
    WHERE hire_date >= '2025-04-01'
) AS ranked
WHERE rn <= 3
ORDER BY 4;
