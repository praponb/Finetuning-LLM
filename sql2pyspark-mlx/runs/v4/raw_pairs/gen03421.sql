WITH base AS (
    SELECT *, DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_date_label
    FROM employees
    WHERE level <> 'L5' AND manager_id = 21
),
summary AS (
    SELECT DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_month, SUM(CASE WHEN level = 'L2' THEN salary ELSE 0 END) AS l2_salary
    FROM base
    GROUP BY DATE_FORMAT(hire_date, 'yyyy-MM')
)
SELECT hire_month, l2_salary
FROM summary
WHERE l2_salary < 80
