SELECT level, MAX(hire_date) AS latest_hire_date FROM employees WHERE level IN ('L1', 'L2', 'L3') GROUP BY level;
