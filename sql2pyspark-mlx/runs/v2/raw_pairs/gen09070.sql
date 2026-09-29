SELECT MONTH(hire_date) hire_month, manager_id, SUM(CASE WHEN level = 'L4' THEN salary ELSE 0 END) l4_salary
FROM employees
WHERE manager_id IS NOT NULL OR dept_id IN (6, 30, 37)
GROUP BY MONTH(hire_date), manager_id
ORDER BY l4_salary
