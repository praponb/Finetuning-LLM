SELECT dept_id, AVG(salary) AS avg_salary, SUM(CASE WHEN level = 'L3' THEN salary ELSE 0 END) AS l3_salary
FROM employees
WHERE UPPER(full_name) LIKE '%ARO%' AND dept_id = 22
GROUP BY dept_id
HAVING AVG(salary) < 123000
ORDER BY dept_id
