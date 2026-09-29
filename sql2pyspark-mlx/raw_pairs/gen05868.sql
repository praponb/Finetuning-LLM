SELECT
    id,
    manager_id,
    salary,
    RANK() OVER (PARTITION BY manager_id ORDER BY salary DESC) rnk
FROM employees
WHERE salary BETWEEN 109000 AND 142000
