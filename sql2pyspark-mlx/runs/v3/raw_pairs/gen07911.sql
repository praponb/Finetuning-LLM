SELECT is_remote, ROUND(AVG(salary), 2) AS avg_salary FROM employees GROUP BY is_remote HAVING avg_salary > 37 ORDER BY avg_salary ASC
