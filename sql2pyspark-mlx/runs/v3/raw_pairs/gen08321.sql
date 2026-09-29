SELECT
    is_remote,
    YEAR(hire_date) AS hire_year,
    MONTH(hire_date) AS hire_month,
    SUM(CASE WHEN level = 'L3' THEN salary ELSE 0 END) AS l3_salary
FROM employees
GROUP BY is_remote, YEAR(hire_date), MONTH(hire_date)
ORDER BY hire_year DESC, is_remote, hire_month
LIMIT 50
