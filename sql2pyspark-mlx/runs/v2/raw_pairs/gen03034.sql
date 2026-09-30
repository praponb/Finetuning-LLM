WITH src AS (
    SELECT *
    FROM employees
    WHERE level NOT IN ('L2', 'L3', 'L1')
),
summary AS (
    SELECT job_title, ROUND(STDDEV(salary), 2) AS stddev_salary, MIN(salary) AS min_salary
    FROM src
    GROUP BY job_title
)
SELECT job_title, stddev_salary, min_salary
FROM summary
WHERE stddev_salary >= 49
ORDER BY min_salary
