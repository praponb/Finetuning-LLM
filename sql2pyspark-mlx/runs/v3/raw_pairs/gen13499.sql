SELECT DATE_FORMAT(hire_date, 'yyyy-MM') hire_month, is_remote, dept_id, COUNT(*) row_count FROM employees WHERE salary > 94000 GROUP BY DATE_FORMAT(hire_date, 'yyyy-MM'), is_remote, dept_id;
