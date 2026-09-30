SELECT is_remote, job_title, ROUND(AVG(salary), 2) avg_salary, MIN(salary) min_salary FROM employees GROUP BY is_remote, job_title HAVING MIN(salary) >= 138000 ORDER BY avg_salary DESC, job_title;
