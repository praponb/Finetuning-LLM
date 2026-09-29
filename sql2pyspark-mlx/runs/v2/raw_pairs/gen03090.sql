SELECT
    manager_id,
    SUM(salary) total_salary,
    COUNT(*) cnt,
    ROUND(SUM(salary) * 100.0 / SUM(SUM(salary)) OVER (), 2) AS pct_of_total
FROM employees
GROUP BY manager_id
ORDER BY cnt DESC
