SELECT is_remote, COUNT(DISTINCT salary) AS unique_managers FROM employees WHERE level = 'L1' GROUP BY is_remote HAVING unique_managers >= 4;
