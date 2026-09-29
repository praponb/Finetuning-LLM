SELECT CASE WHEN d.budget >= 3759000 THEN 'high' WHEN d.budget >= 851000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(*) AS row_count
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.id
GROUP BY CASE WHEN d.budget >= 3759000 THEN 'high' WHEN d.budget >= 851000 THEN 'mid' ELSE 'low' END
ORDER BY row_count DESC, budget_bucket
LIMIT 50;
