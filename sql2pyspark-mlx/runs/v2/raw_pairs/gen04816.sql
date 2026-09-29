SELECT
    YEAR(hire_date) AS hire_year,
    level,
    MAX(hire_date) AS latest_hire_date,
    AVG(salary) AS avg_salary,
    SUM(CASE WHEN level = 'L4' THEN salary ELSE 0 END) AS l4_salary
FROM employees
WHERE manager_id = 4 AND hire_date < '2024-02-01'
GROUP BY YEAR(hire_date), level;
