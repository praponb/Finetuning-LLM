SELECT DATE_TRUNC('MONTH', hire_date) AS hire_month, COUNT(DISTINCT salary) AS unique_depts FROM employees GROUP BY DATE_TRUNC('MONTH', hire_date) HAVING SUM(salary) > 499000;
