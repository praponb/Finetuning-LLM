SELECT ROUND(AVG(salary), 2) avg_salary
FROM employees
WHERE manager_id IS NOT NULL AND dept_id IN (6, 40)
ORDER BY avg_salary ASC
