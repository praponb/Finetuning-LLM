SELECT
    is_remote,
    MONTH(hire_date) AS hire_month,
    COUNT(CASE WHEN level = 'L1' THEN 1 END) AS l1_count,
    MAX(salary) AS max_salary
FROM employees
WHERE salary < 58000
GROUP BY 1, 2
HAVING MAX(salary) > 74000
