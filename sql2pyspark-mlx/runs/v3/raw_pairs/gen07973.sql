SELECT is_remote, ROUND((100 * SUM(CASE WHEN level = 'L1' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l1 FROM employees WHERE manager_id IS NOT NULL GROUP BY 1
