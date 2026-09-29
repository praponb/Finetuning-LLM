SELECT *
FROM employees
QUALIFY RANK() OVER (PARTITION BY manager_id, dept_id ORDER BY salary) = 1
