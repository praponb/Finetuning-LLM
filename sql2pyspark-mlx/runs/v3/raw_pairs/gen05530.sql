SELECT hire_quarter, hire_year, l3_salary
FROM (
    SELECT QUARTER(hire_date) AS hire_quarter, YEAR(hire_date) AS hire_year, SUM(CASE WHEN level = 'L3' THEN salary ELSE 0 END) AS l3_salary
    FROM employees
    GROUP BY QUARTER(hire_date), YEAR(hire_date)
) AS s
WHERE l3_salary >= 52
ORDER BY hire_quarter DESC, hire_year ASC
LIMIT 5;
