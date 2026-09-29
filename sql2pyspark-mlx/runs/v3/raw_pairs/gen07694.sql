WITH valid_employees AS (
    SELECT *, ROUND(salary * 1.07, 2) adjusted_salary
    FROM employees
    WHERE hire_date < '2023-11-01'
),
Summary AS (
    SELECT level, SUM(salary) total_salary
    FROM valid_employees
    GROUP BY level
)
SELECT level, total_salary
FROM Summary
WHERE total_salary > 415000
