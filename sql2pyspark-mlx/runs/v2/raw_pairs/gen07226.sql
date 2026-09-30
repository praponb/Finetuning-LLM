SELECT d.location, SUM(e.salary) AS total_salary FROM employees e LEFT JOIN departments d ON e.dept_id = d.id WHERE e.level <> 'L1' GROUP BY d.location HAVING SUM(e.salary) >= 462000
