SELECT x.country, y.job_title FROM (SELECT DISTINCT country FROM customers) x CROSS JOIN (SELECT DISTINCT job_title FROM employees) y ORDER BY country, job_title
