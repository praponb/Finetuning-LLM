WITH filtered_employees AS (
    SELECT *, CASE WHEN salary >= 138000 THEN 'High' WHEN salary >= 73000 THEN 'Medium' ELSE 'Low' END AS salary_tier
    FROM employees
    WHERE salary < 143000 AND salary < 74000
),
hire_month_summary AS (
    SELECT
        MONTH(hire_date) AS hire_month,
        manager_id,
        COUNT(DISTINCT salary) AS unique_levels,
        SUM(CASE WHEN level = 'L5' THEN salary ELSE 0 END) AS l5_salary
    FROM filtered_employees
    GROUP BY MONTH(hire_date), manager_id
)
SELECT
    hire_month,
    manager_id,
    unique_levels,
    l5_salary
FROM hire_month_summary
