SELECT
    YEAR(hire_date) AS hire_year,
    SUM(salary) AS total_salary,
    COUNT(*) AS cnt,
    ROUND(SUM(salary) * 100.0 / SUM(SUM(salary)) OVER (), 2) AS pct_of_total
FROM employees
WHERE level = 'L5'
GROUP BY YEAR(hire_date)
ORDER BY pct_of_total;
