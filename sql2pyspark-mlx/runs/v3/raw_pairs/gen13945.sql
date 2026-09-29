SELECT budget_bucket, avg_budget, max_budget
FROM (
    SELECT CASE WHEN budget >= 3859000 THEN 'high' WHEN budget >= 1721000 THEN 'mid' ELSE 'low' END AS budget_bucket, AVG(budget) AS avg_budget, MAX(budget) AS max_budget
    FROM departments
    WHERE dept_name IN ('Marketing', 'Engineering', 'HR')
    GROUP BY CASE WHEN budget >= 3859000 THEN 'high' WHEN budget >= 1721000 THEN 'mid' ELSE 'low' END
) AS t
WHERE max_budget >= 2669000;
