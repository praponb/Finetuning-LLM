SELECT budget_bucket, max_budget, dept_name_list
FROM (
    SELECT CASE WHEN budget >= 3132000 THEN 'high' WHEN budget >= 1594000 THEN 'mid' ELSE 'low' END AS budget_bucket, MAX(budget) AS max_budget, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
    FROM departments
    GROUP BY CASE WHEN budget >= 3132000 THEN 'high' WHEN budget >= 1594000 THEN 'mid' ELSE 'low' END
) AS sub
WHERE max_budget < 1625000
ORDER BY budget_bucket;
