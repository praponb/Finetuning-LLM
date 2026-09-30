SELECT MAX(salary) AS max_salary
FROM employees
WHERE level IN ('L5', 'L3', 'L1') AND manager_id IN (18, 24);
