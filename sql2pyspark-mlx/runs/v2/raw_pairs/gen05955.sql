SELECT level, job_title, COUNT(DISTINCT salary) AS unique_levels, MAX(salary) - MIN(salary) AS salary_range FROM employees GROUP BY ROLLUP(level, job_title) ORDER BY job_title ASC, level ASC
