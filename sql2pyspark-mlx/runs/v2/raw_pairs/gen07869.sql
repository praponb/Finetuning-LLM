SELECT
    id,
    salary,
    level,
    full_name
FROM employees
WHERE salary >= (SELECT AVG(salary) FROM employees);
