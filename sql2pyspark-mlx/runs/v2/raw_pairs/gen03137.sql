SELECT
    YEAR(hire_date) AS hire_year,
    MAX(salary) AS max_salary,
    MAX(salary) - MIN(salary) AS salary_range,
    SUM(CASE WHEN level = 'L2' THEN salary ELSE 0 END) AS l2_salary
FROM employees
WHERE UPPER(TRIM(email)) = 'IVY@GMAIL.COM'
GROUP BY 1
