SELECT job_title, unique_depts FROM (SELECT job_title, COUNT(DISTINCT salary) AS unique_depts FROM employees GROUP BY job_title) AS agg WHERE unique_depts < 4 ORDER BY job_title
