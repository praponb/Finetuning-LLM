SELECT hire_date, salary, is_remote, CAST(is_remote AS INT) AS is_remote_flag, CAST(salary AS INT) AS salary_int FROM employees ORDER BY is_remote_flag
