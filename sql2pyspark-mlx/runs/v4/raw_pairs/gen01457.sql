WITH src AS (
    SELECT *, DATE_ADD(hire_date, 88) AS hire_date_plus_88d
    FROM employees
    WHERE manager_id = 4
),
summary AS (
    SELECT YEAR(hire_date_plus_88d) AS hire_plus_88d_year, MAX(salary) - MIN(salary) AS salary_range, ROUND(STDDEV(salary), 2) AS stddev_salary
    FROM src
    GROUP BY YEAR(hire_date_plus_88d)
)
SELECT hire_plus_88d_year, salary_range
FROM summary
ORDER BY salary_range ASC, hire_plus_88d_year DESC
