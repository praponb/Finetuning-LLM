SELECT level, MAX(salary) - MIN(salary) AS salary_range FROM employees WHERE is_remote AND level IN ('L5', 'L3') GROUP BY level;
