SELECT CASE WHEN d.budget >= 3700000 THEN 'high' WHEN d.budget >= 1256000 THEN 'mid' ELSE 'low' END budget_bucket, ROUND((100 * SUM(CASE WHEN e.job_title = 'Intern' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_intern, COUNT(*) num_employees
FROM employees e
LEFT OUTER JOIN departments d ON e.dept_id = d.id
GROUP BY CASE WHEN d.budget >= 3700000 THEN 'high' WHEN d.budget >= 1256000 THEN 'mid' ELSE 'low' END
ORDER BY 3 DESC, budget_bucket
