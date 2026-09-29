SELECT is_remote, SUM(salary) AS total_salary, ROUND(SUM(salary) * 100.0 / SUM(SUM(salary)) OVER (), 2) AS pct_of_total
FROM employees
GROUP BY is_remote
ORDER BY pct_of_total ASC, total_salary ASC;
