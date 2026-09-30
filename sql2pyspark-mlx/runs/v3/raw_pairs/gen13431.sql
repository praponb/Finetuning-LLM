WITH recent_employees AS (
    SELECT *, ADD_MONTHS(hire_date, 3) AS hire_date_plus_months
    FROM employees
    WHERE hire_date >= '2024-12-01' AND is_remote
),
employees_stats AS (
    SELECT CASE WHEN level IN ('L2', 'L3') THEN 'tier_1' ELSE 'other' END AS level_group, SUM(salary) AS total_salary
    FROM recent_employees
    GROUP BY CASE WHEN level IN ('L2', 'L3') THEN 'tier_1' ELSE 'other' END
)
SELECT *
FROM employees_stats
ORDER BY level_group
LIMIT 5;
