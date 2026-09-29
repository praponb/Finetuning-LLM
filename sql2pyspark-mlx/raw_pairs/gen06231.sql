SELECT is_remote, AVG(salary) avg_salary FROM employees WHERE level = 'L1' AND LENGTH(full_name) < 12 AND job_title = 'Manager' GROUP BY is_remote HAVING AVG(salary) > 131000
