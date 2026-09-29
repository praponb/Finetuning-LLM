SELECT a.city, b.job_title FROM (SELECT DISTINCT city FROM customers) a CROSS JOIN (SELECT DISTINCT job_title FROM employees) b ORDER BY city, job_title;
