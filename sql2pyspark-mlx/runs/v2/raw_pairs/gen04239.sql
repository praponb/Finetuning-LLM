SELECT *
FROM (
    SELECT manager_id, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY manager_id
) AS s
WHERE avg_salary > 100000;
