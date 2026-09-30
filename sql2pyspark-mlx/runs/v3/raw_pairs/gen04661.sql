SELECT x.segment, y.job_title FROM (SELECT DISTINCT segment FROM customers) x CROSS JOIN (SELECT DISTINCT job_title FROM employees) y ORDER BY segment, job_title
