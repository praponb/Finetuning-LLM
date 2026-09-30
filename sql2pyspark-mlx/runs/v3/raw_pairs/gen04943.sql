WITH valid_employees AS (
    SELECT *
    FROM employees
    WHERE email NOT LIKE '%@outlook.com' AND YEAR(hire_date) >= 2025
),
hire_quarter_summary AS (
    SELECT QUARTER(hire_date) AS hire_quarter, MAX(salary) AS max_salary, MIN(salary) AS min_salary
    FROM valid_employees
    GROUP BY QUARTER(hire_date)
)
SELECT *
FROM hire_quarter_summary
ORDER BY hire_quarter, max_salary;
