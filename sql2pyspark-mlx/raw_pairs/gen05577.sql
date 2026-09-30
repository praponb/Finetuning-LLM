WITH src AS (
    SELECT *, FLOOR(salary) salary_whole
    FROM employees
    WHERE dept_id IN (4, 17, 29, 36) AND email IS NOT NULL
),
EmployeesStats AS (
    SELECT YEAR(hire_date) hire_year, MIN(salary_whole) min_salary_whole, MAX(salary_whole) max_salary_whole
    FROM src
    GROUP BY YEAR(hire_date)
)
SELECT hire_year, min_salary_whole
FROM EmployeesStats
WHERE min_salary_whole > 40
ORDER BY hire_year DESC;
