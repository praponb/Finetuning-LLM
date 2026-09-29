SELECT manager_id, SUM(salary) AS total_salary, DENSE_RANK() OVER (ORDER BY SUM(salary) DESC) AS rnk
FROM employees
WHERE full_name LIKE '%Pim%'
GROUP BY manager_id
ORDER BY rnk DESC, manager_id
LIMIT 5;
