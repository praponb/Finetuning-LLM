WITH src AS (
    SELECT *
    FROM employees
    WHERE manager_id = 21 AND (dept_id = 12 OR salary <= 57000)
),
employees_stats AS (
    SELECT manager_id, MAX(hire_date) AS latest_hire_date, ROUND(STDDEV(salary), 2) AS stddev_salary
    FROM src
    GROUP BY manager_id
)
SELECT manager_id, latest_hire_date
FROM employees_stats
WHERE stddev_salary > 46
ORDER BY manager_id DESC
